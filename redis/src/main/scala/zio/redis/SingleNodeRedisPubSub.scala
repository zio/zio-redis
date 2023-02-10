package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeRedisPubSub.{Request, RequestQueueSize, SubscriptionKey, True}
import zio.redis.api.PubSub
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, Promise, Queue, Ref, Schedule, UIO, ZIO}

final class SingleNodeRedisPubSub(
  pubSubHubsRef: Ref[Map[SubscriptionKey, Hub[PushProtocol]]],
  unsubscribedRef: Ref[Map[SubscriptionKey, Promise[RedisError, PushProtocol]]],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends RedisPubSub {

  def execute(command: PubSubCommand): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    command match {
      case PubSubCommand.Subscribe(channel, channels) =>
        subscribe(channel, channels)
      case PubSubCommand.PSubscribe(pattern, patterns) =>
        pSubscribe(pattern, patterns)
      case PubSubCommand.Unsubscribe(channels)  => unsubscribe(channels)
      case PubSubCommand.PUnsubscribe(patterns) => pUnsubscribe(patterns)
    }

  private def subscribe(
    channel: String,
    channels: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.Subscribe,
      channelKey(channel),
      channels.map(channelKey(_))
    )

  private def pSubscribe(
    pattern: String,
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.PSubscribe,
      patternKey(pattern),
      patterns.map(patternKey(_))
    )

  private def unsubscribe(
    channels: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeUnsubscriptionStream(
      PubSub.Unsubscribe,
      if (channels.nonEmpty)
        ZIO.succeedNow(channels.map(channelKey(_)))
      else
        pubSubHubsRef.get.map(_.keys.filter(_.isChannel).toList)
    )

  private def pUnsubscribe(
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeUnsubscriptionStream(
      PubSub.PUnsubscribe,
      if (patterns.nonEmpty)
        ZIO.succeedNow(patterns.map(patternKey(_)))
      else
        pubSubHubsRef.get.map(_.keys.filter(_.isPattern).toList)
    )

  private def makeSubscriptionStream(
    command: String,
    key: SubscriptionKey,
    keys: List[SubscriptionKey]
  ) =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        promise <- Promise.make[RedisError, Unit]
        chunk    = StringInput.encode(command) ++ NonEmptyList(StringInput).encode((key.value, keys.map(_.value)))
        streams <- makeStreams(key :: keys)
        _       <- reqQueue.offer(Request(chunk, promise))
        _       <- promise.await
      } yield streams
    }

  private def makeUnsubscriptionStream(
    command: String,
    keys: UIO[List[SubscriptionKey]]
  ) =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        targets <- keys
        chunk    = StringInput.encode(command) ++ Varargs(StringInput).encode(targets.map(_.value))
        promise <- Promise.make[RedisError, Unit]
        _       <- reqQueue.offer(Request(chunk, promise))
        _       <- promise.await
        streams <-
          ZIO.foreach(targets)(key =>
            for {
              promise <- Promise.make[RedisError, PushProtocol]
              _       <- unsubscribedRef.update(_ + (key -> promise))
            } yield ZStream.fromZIO(promise.await)
          )
      } yield streams
    }

  private def makeStreams(keys: List[SubscriptionKey]): UIO[List[Stream[RedisError, PushProtocol]]] =
    ZIO.foreach(keys)(getHub(_).map(ZStream.fromHub(_)))

  private def getHub(key: SubscriptionKey) = {
    def makeNewHub =
      Hub
        .unbounded[PushProtocol]
        .tap(hub => pubSubHubsRef.update(_ + (key -> hub)))

    for {
      hubs <- pubSubHubsRef.get
      hub  <- ZIO.fromOption(hubs.get(key)).orElse(makeNewHub)
    } yield hub
  }

  private def send =
    reqQueue.takeBetween(1, RequestQueueSize).flatMap { reqs =>
      val buffer = ChunkBuilder.make[Byte]()
      val it     = reqs.iterator

      while (it.hasNext) {
        val req = it.next()
        buffer ++= RespValue.Array(req.command).serialize
      }

      val bytes = buffer.result()

      connection
        .write(bytes)
        .mapError(RedisError.IOError(_))
        .tapBoth(
          e => ZIO.foreachDiscard(reqs)(_.promise.fail(e)),
          _ => ZIO.foreachDiscard(reqs)(_.promise.succeed(()))
        )
    }

  private def receive: ZIO[BinaryCodec, RedisError, Unit] = {
    def releaseHub(key: SubscriptionKey) =
      for {
        pubSubs <- pubSubHubsRef.get
        hubOpt   = pubSubs.get(key)
        _       <- ZIO.fromOption(hubOpt).flatMap(_.shutdown).orElse(ZIO.unit)
        _       <- pubSubHubsRef.update(_ - key)
      } yield ()

    def handlePushProtocolMessage(msg: PushProtocol) = msg match {
      case msg @ PushProtocol.Unsubscribe(channel, _) =>
        for {
          _   <- releaseHub(channelKey(channel))
          map <- unsubscribedRef.get
          promise <- ZIO
                       .fromOption(map.get(channelKey(channel)))
                       .orElseFail(RedisError.NoUnsubscribeRequest(channel))
          _ <- promise.succeed(msg)
        } yield ()
      case msg @ PushProtocol.PUnsubscribe(pattern, _) =>
        for {
          _   <- releaseHub(patternKey(pattern))
          map <- unsubscribedRef.get
          promise <- ZIO
                       .fromOption(map.get(patternKey(pattern)))
                       .orElseFail(RedisError.NoUnsubscribeRequest(pattern))
          _ <- promise.succeed(msg)
        } yield ()
      case msg @ PushProtocol.Subscribe(channel, _)   => getHub(channelKey(channel)).flatMap(_.offer(msg))
      case msg @ PushProtocol.PSubscribe(pattern, _)  => getHub(patternKey(pattern)).flatMap(_.offer(msg))
      case msg @ PushProtocol.Message(channel, _)     => getHub(channelKey(channel)).flatMap(_.offer(msg))
      case msg @ PushProtocol.PMessage(pattern, _, _) => getHub(patternKey(pattern)).flatMap(_.offer(msg))
    }

    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      connection.read
        .mapError(RedisError.IOError(_))
        .via(RespValue.decoder)
        .collectSome
        .mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp)))
        .refineToOrDie[RedisError]
        .foreach(push => handlePushProtocolMessage(push))
    }
  }

  private def resubscribe: ZIO[BinaryCodec, RedisError, Unit] =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      def makeCommand(name: String, keys: Set[String]) =
        RespValue.Array(StringInput.encode(name) ++ Varargs(StringInput).encode(keys)).serialize

      for {
        keySet              <- pubSubHubsRef.get.map(_.keySet)
        (channels, patterns) = keySet.partition(_.isChannel)
        _ <- (connection.write(makeCommand(PubSub.Subscribe, channels.map(_.value))).when(channels.nonEmpty) *>
               connection.write(makeCommand(PubSub.PSubscribe, patterns.map(_.value))).when(patterns.nonEmpty))
               .mapError(RedisError.IOError(_))
               .retryWhile(True)
      } yield ()
    }

  private def patternKey(key: String) = SubscriptionKey(key, true)
  private def channelKey(key: String) = SubscriptionKey(key, false)

  /**
   * Opens a connection to the server and launches receive operations. All failures are retried by opening a new
   * connection. Only exits by interruption or defect.
   */
  val run: ZIO[BinaryCodec, RedisError, AnyVal] =
    ZIO.logTrace(s"$this Executable sender and reader has been started") *>
      (send.repeat[BinaryCodec, Long](Schedule.forever) race receive)
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e") *> resubscribe)
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))
}

object SingleNodeRedisPubSub {
  private final case class SubscriptionKey(value: String, isPattern: Boolean) {
    def isChannel: Boolean = isPattern == false
  }
  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, Unit]
  )

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection) =
    for {
      hubRef          <- Ref.make(Map.empty[SubscriptionKey, Hub[PushProtocol]])
      unsubscribedRef <- Ref.make(Map.empty[SubscriptionKey, Promise[RedisError, PushProtocol]])
      reqQueue        <- Queue.bounded[Request](RequestQueueSize)
      pubSub           = new SingleNodeRedisPubSub(hubRef, unsubscribedRef, reqQueue, conn)
      _               <- pubSub.run.forkScoped
      _               <- logScopeFinalizer(s"$pubSub Node PubSub is closed")
    } yield pubSub
}
