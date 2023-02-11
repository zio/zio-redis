package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeRedisPubSub.{Request, RequestQueueSize, SubscriptionKey, True}
import zio.redis.api.PubSub
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, Promise, Queue, Ref, Schedule, UIO, ZIO}

final class SingleNodeRedisPubSub(
  pubSubHubsRef: Ref[Map[SubscriptionKey, Hub[Take[RedisError, PushProtocol]]]],
  reqQueue: Queue[Request],
  resQueue: Queue[Promise[RedisError, PushProtocol]],
  connection: RedisConnection
) extends RedisPubSub {

  def execute(command: PubSubCommand): ZIO[BinaryCodec, RedisError, Chunk[Stream[RedisError, PushProtocol]]] =
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
  ): ZIO[BinaryCodec, RedisError, Chunk[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.Subscribe,
      channelKey(channel),
      channels.map(channelKey(_))
    )

  private def pSubscribe(
    pattern: String,
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, Chunk[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.PSubscribe,
      patternKey(pattern),
      patterns.map(patternKey(_))
    )

  private def unsubscribe(
    channels: List[String]
  ): ZIO[BinaryCodec, RedisError, Chunk[Stream[RedisError, PushProtocol]]] =
    makeUnsubscriptionStream(
      PubSub.Unsubscribe,
      if (channels.nonEmpty)
        ZIO.succeedNow(channels.map(channelKey(_)))
      else
        pubSubHubsRef.get.map(_.keys.filter(_.isChannel).toList)
    )

  private def pUnsubscribe(
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, Chunk[Stream[RedisError, PushProtocol]]] =
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
        promises <- Promise.make[RedisError, PushProtocol].replicateZIO(keys.size + 1).map(Chunk.fromIterable(_))
        chunk     = StringInput.encode(command) ++ NonEmptyList(StringInput).encode((key.value, keys.map(_.value)))
        _        <- reqQueue.offer(Request(chunk, promises))
        streams <- ZIO.foreach((key :: keys) zip promises) { case (key, promise) =>
                     for {
                       hub   <- getHub(key)
                       stream = ZStream.fromHub(hub).flattenTake
                     } yield ZStream.fromZIO(promise.await) concat stream
                   }
      } yield Chunk.fromIterable(streams)
    }

  private def makeUnsubscriptionStream(
    command: String,
    keys: UIO[List[SubscriptionKey]]
  ) =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        targets  <- keys
        chunk     = StringInput.encode(command) ++ Varargs(StringInput).encode(targets.map(_.value))
        promises <- Promise.make[RedisError, PushProtocol].replicateZIO(targets.size).map(Chunk.fromIterable(_))
        _        <- reqQueue.offer(Request(chunk, promises))
        streams   = promises.map(promise => ZStream.fromZIO(promise.await))
      } yield streams
    }

  private def getHub(key: SubscriptionKey) = {
    def makeNewHub =
      Hub
        .unbounded[Take[RedisError, PushProtocol]]
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
          e => ZIO.foreachDiscard(reqs.flatMap(_.promises))(_.fail(e)),
          _ => ZIO.foreachDiscard(reqs.map(_.promises))(resQueue.offerAll(_))
        )
    }

  private def receive: ZIO[BinaryCodec, RedisError, Unit] = {
    def handlePushProtocolMessage(msg: PushProtocol): UIO[Unit] = {
      def releasePendingPromise(msg: PushProtocol): UIO[Unit] =
        resQueue.take.flatMap(_.succeed(msg)).unit

      def handleUnsubscription(key: SubscriptionKey, msg: PushProtocol): UIO[Unit] =
        for {
          _       <- releasePendingPromise(msg)
          pubSubs <- pubSubHubsRef.get
          hubOpt   = pubSubs.get(key)
          _       <- ZIO.fromOption(hubOpt).flatMap(_.offer(Take.end)).orElse(ZIO.unit)
          _       <- pubSubHubsRef.update(_ - key)
        } yield ()

      def handleSubscription(key: SubscriptionKey, msg: PushProtocol): UIO[Unit] =
        for {
          _ <- resQueue.take.flatMap(_.succeed(msg))
          _ <- getHub(key).flatMap(_.offer(Take.single(msg)))
        } yield ()

      msg match {
        case msg @ PushProtocol.Unsubscribe(channel, _) =>
          handleUnsubscription(channelKey(channel), msg)
        case msg @ PushProtocol.PUnsubscribe(pattern, _) =>
          handleUnsubscription(patternKey(pattern), msg)
        case msg @ PushProtocol.Subscribe(channel, _) =>
          handleSubscription(channelKey(channel), msg)
        case msg @ PushProtocol.PSubscribe(pattern, _) =>
          handleSubscription(patternKey(pattern), msg)
        case msg @ PushProtocol.Message(channel, _) =>
          getHub(channelKey(channel)).flatMap(_.offer(Take.single(msg))).unit
        case msg @ PushProtocol.PMessage(pattern, _, _) =>
          getHub(patternKey(pattern)).flatMap(_.offer(Take.single(msg))).unit
      }
    }

    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      connection.read
        .mapError(RedisError.IOError(_))
        .via(RespValue.decoder)
        .collectSome
        .mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp)))
        .refineToOrDie[RedisError]
        .foreach(handlePushProtocolMessage(_))
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
    promises: Chunk[Promise[RedisError, PushProtocol]]
  )

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection) =
    for {
      hubRef   <- Ref.make(Map.empty[SubscriptionKey, Hub[Take[RedisError, PushProtocol]]])
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      resQueue <- Queue.unbounded[Promise[RedisError, PushProtocol]]
      pubSub    = new SingleNodeRedisPubSub(hubRef, reqQueue, resQueue, conn)
      _        <- pubSub.run.forkScoped
      _        <- logScopeFinalizer(s"$pubSub Node PubSub is closed")
    } yield pubSub
}
