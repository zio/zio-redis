package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeRedisPubSub.{Request, RequestQueueSize, True}
import zio.redis.api.PubSub
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, IO, Promise, Queue, Ref, Schedule, UIO, ZIO}

final class SingleNodeRedisPubSub(
  pubSubHubsRef: Ref[Map[SubscriptionKey, Hub[PushProtocol]]],
  callbacksRef: Ref[Map[SubscriptionKey, Chunk[PubSubCallback]]],
  unsubscribedRef: Ref[Map[SubscriptionKey, Promise[RedisError, PushProtocol]]],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends RedisPubSub {

  def execute(command: PubSubCommand): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    command match {
      case PubSubCommand.Subscribe(channel, channels, onSubscribe) =>
        subscribe(channel, channels, onSubscribe)
      case PubSubCommand.PSubscribe(pattern, patterns, onSubscribe) =>
        pSubscribe(pattern, patterns, onSubscribe)
      case PubSubCommand.Unsubscribe(channels)  => unsubscribe(channels)
      case PubSubCommand.PUnsubscribe(patterns) => pUnsubscribe(patterns)
    }

  private def subscribe(
    channel: String,
    channels: List[String],
    onSubscribe: PubSubCallback
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.Subscribe,
      SubscriptionKey.Channel(channel),
      channels.map(SubscriptionKey.Channel(_)),
      onSubscribe
    )

  private def pSubscribe(
    pattern: String,
    patterns: List[String],
    onSubscribe: PubSubCallback
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.PSubscribe,
      SubscriptionKey.Pattern(pattern),
      patterns.map(SubscriptionKey.Pattern(_)),
      onSubscribe
    )

  private def unsubscribe(
    channels: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeUnsubscriptionStream(
      PubSub.Unsubscribe,
      if (channels.nonEmpty)
        ZIO.succeedNow(channels.map(SubscriptionKey.Channel(_)))
      else
        pubSubHubsRef.get.map(_.keys.filter(_.isChannelKey).toList)
    )

  private def pUnsubscribe(
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeUnsubscriptionStream(
      PubSub.PUnsubscribe,
      if (patterns.nonEmpty)
        ZIO.succeedNow(patterns.map(SubscriptionKey.Pattern(_)))
      else
        pubSubHubsRef.get.map(_.keys.filter(_.isPatternKey).toList)
    )

  private def makeSubscriptionStream(
    command: String,
    key: SubscriptionKey,
    keys: List[SubscriptionKey],
    onSubscribe: PubSubCallback
  ) =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        promise <- Promise.make[RedisError, Unit]
        chunk    = StringInput.encode(command) ++ NonEmptyList(StringInput).encode((key.value, keys.map(_.value)))
        streams <- makeStreams(key :: keys)
        _       <- reqQueue.offer(Request(chunk, promise, Some((key, onSubscribe))))
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
        _       <- reqQueue.offer(Request(chunk, promise, None))
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

  private def send = {
    def registerCallbacks(request: Request) =
      ZIO
        .fromOption(request.callbacks)
        .flatMap { case (key, additionalCallbacks) =>
          for {
            callbackMap <- callbacksRef.get
            callbacks    = callbackMap.getOrElse(key, Chunk.empty)
            _           <- callbacksRef.update(_.updated(key, callbacks appended additionalCallbacks))
          } yield ()
        }
        .orElse(ZIO.unit)

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
          _ => ZIO.foreachDiscard(reqs)(req => registerCallbacks(req) *> req.promise.succeed(()))
        )
    }
  }

  private def receive: ZIO[BinaryCodec, RedisError, Unit] = {
    def applySubscriptionCallback(protocol: PushProtocol): IO[RedisError, Unit] = {
      def runAndCleanup(key: SubscriptionKey, numOfSubscription: Long) =
        for {
          callbackMap <- callbacksRef.get
          callbacks    = callbackMap.getOrElse(key, Chunk.empty)
          _           <- ZIO.foreachDiscard(callbacks)(_(key.value, numOfSubscription))
          _           <- callbacksRef.update(_.updated(key, Chunk.empty))
        } yield ()

      protocol match {
        case PushProtocol.Subscribe(channel, numOfSubscription) =>
          runAndCleanup(SubscriptionKey.Channel(channel), numOfSubscription)
        case PushProtocol.PSubscribe(pattern, numOfSubscription) =>
          runAndCleanup(SubscriptionKey.Pattern(pattern), numOfSubscription)
        case _ => ZIO.unit
      }
    }

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
          _   <- releaseHub(SubscriptionKey.Channel(channel))
          map <- unsubscribedRef.get
          promise <- ZIO
                       .fromOption(map.get(SubscriptionKey.Channel(channel)))
                       .orElseFail(RedisError.NoUnsubscribeRequest(channel))
          _ <- promise.succeed(msg)
        } yield ()
      case msg @ PushProtocol.PUnsubscribe(pattern, _) =>
        for {
          _   <- releaseHub(SubscriptionKey.Pattern(pattern))
          map <- unsubscribedRef.get
          promise <- ZIO
                       .fromOption(map.get(SubscriptionKey.Pattern(pattern)))
                       .orElseFail(RedisError.NoUnsubscribeRequest(pattern))
          _ <- promise.succeed(msg)
        } yield ()
      case other => getHub(other.key).flatMap(_.offer(other))
    }

    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      connection.read
        .mapError(RedisError.IOError(_))
        .via(RespValue.decoder)
        .collectSome
        .mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp)))
        .refineToOrDie[RedisError]
        .foreach(push => applySubscriptionCallback(push) *> handlePushProtocolMessage(push))
    }
  }

  private def resubscribe: ZIO[BinaryCodec, RedisError, Unit] =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      def makeCommand(name: String, keys: Set[String]) =
        RespValue.Array(StringInput.encode(name) ++ Varargs(StringInput).encode(keys)).serialize

      for {
        keySet              <- pubSubHubsRef.get.map(_.keySet)
        (channels, patterns) = keySet.partition(_.isChannelKey)
        _ <- (connection.write(makeCommand(PubSub.Subscribe, channels.map(_.value))).when(channels.nonEmpty) *>
               connection.write(makeCommand(PubSub.PSubscribe, patterns.map(_.value))).when(patterns.nonEmpty))
               .mapError(RedisError.IOError(_))
               .retryWhile(True)
      } yield ()
    }

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
  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, Unit],
    callbacks: Option[(SubscriptionKey, PubSubCallback)]
  )

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection) =
    for {
      hubRef          <- Ref.make(Map.empty[SubscriptionKey, Hub[PushProtocol]])
      callbackRef     <- Ref.make(Map.empty[SubscriptionKey, Chunk[PubSubCallback]])
      unsubscribedRef <- Ref.make(Map.empty[SubscriptionKey, Promise[RedisError, PushProtocol]])
      reqQueue        <- Queue.bounded[Request](RequestQueueSize)
      pubSub           = new SingleNodeRedisPubSub(hubRef, callbackRef, unsubscribedRef, reqQueue, conn)
      _               <- pubSub.run.forkScoped
      _               <- logScopeFinalizer(s"$pubSub Node PubSub is closed")
    } yield pubSub
}
