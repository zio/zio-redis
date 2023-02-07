package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeRedisPubSub.{Request, RequestQueueSize, True}
import zio.redis.api.PubSub
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, Promise, Queue, Ref, Schedule, UIO, ZIO}

final class SingleNodeRedisPubSub(
  pubSubHubsRef: Ref[Map[SubscriptionKey, Hub[PushProtocol]]],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends RedisPubSub {

  def execute(command: PubSubCommand): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    command match {
      case PubSubCommand.Subscribe(channel, channels)  => subscribe(channel, channels)
      case PubSubCommand.PSubscribe(pattern, patterns) => pSubscribe(pattern, patterns)
      case PubSubCommand.Unsubscribe(channels)         => unsubscribe(channels)
      case PubSubCommand.PUnsubscribe(patterns)        => pUnsubscribe(patterns)
    }

  private def subscribe(
    channel: String,
    channels: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(PubSub.Subscribe, SubscriptionKey.Channel(channel), channels.map(SubscriptionKey.Channel(_)))

  private def pSubscribe(
    pattern: String,
    patterns: List[String]
  ): ZIO[BinaryCodec, RedisError, List[Stream[RedisError, PushProtocol]]] =
    makeSubscriptionStream(
      PubSub.PSubscribe,
      SubscriptionKey.Pattern(pattern),
      patterns.map(SubscriptionKey.Pattern(_))
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

  private def makeSubscriptionStream(command: String, key: SubscriptionKey, keys: List[SubscriptionKey]) =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        promise <- Promise.make[RedisError, Unit]
        chunk    = StringInput.encode(command) ++ NonEmptyList(StringInput).encode((key.value, keys.map(_.value)))
        streams <- makeStreams(key :: keys)
        _       <- reqQueue.offer(Request(chunk, promise))
        _       <- promise.await
      } yield streams
    }

  private def makeUnsubscriptionStream(command: String, keys: UIO[List[SubscriptionKey]]) = {
    def releaseHub(key: SubscriptionKey) =
      for {
        pubSubs <- pubSubHubsRef.get
        hubOpt   = pubSubs.get(key)
        _       <- ZIO.fromOption(hubOpt).flatMap(_.shutdown).ignore
        _       <- pubSubHubsRef.update(_ - key)
      } yield ()

    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      for {
        targets  <- keys
        chunk     = StringInput.encode(command) ++ Varargs(StringInput).encode(targets.map(_.value))
        promise  <- Promise.make[RedisError, Unit]
        streams  <- makeStreams(targets)
        targetSet = targets.map(_.value).toSet
        _        <- reqQueue.offer(Request(chunk, promise))
        _        <- promise.await
      } yield streams.map(_.tap {
        case PushProtocol.Unsubscribe(channel, _) if targetSet contains channel =>
          releaseHub(SubscriptionKey.Channel(channel))
        case PushProtocol.PUnsubscribe(pattern, _) if targetSet contains pattern =>
          releaseHub(SubscriptionKey.Pattern(pattern))
        case _ => ZIO.unit
      })
    }
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

  private def receive: ZIO[BinaryCodec, RedisError, Unit] =
    ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
      connection.read
        .mapError(RedisError.IOError(_))
        .via(RespValue.decoder)
        .collectSome
        .mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp)))
        .refineToOrDie[RedisError]
        .foreach(push => getHub(push.key).flatMap(_.offer(push)))
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
  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, Unit])

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection) =
    for {
      hubRef   <- Ref.make(Map.empty[SubscriptionKey, Hub[PushProtocol]])
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      pubSub    = new SingleNodeRedisPubSub(hubRef, reqQueue, conn)
      _        <- pubSub.run.forkScoped
      _        <- logScopeFinalizer(s"$pubSub Node PubSub is closed")
    } yield pubSub
}
