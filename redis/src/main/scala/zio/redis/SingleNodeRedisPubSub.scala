package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeRedisPubSub.{Request, RequestQueueSize, True}
import zio.redis.api.PubSub
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, Promise, Queue, Ref, Schedule, UIO, ZIO}

import scala.reflect.ClassTag

final class SingleNodeRedisPubSub(
  pubSubHubsRef: Ref[Map[SubscriptionKey, Hub[PushProtocol]]],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends RedisPubSub {

  def execute(command: RedisPubSubCommand): ZStream[BinaryCodec, RedisError, PushProtocol] =
    command match {
      case RedisPubSubCommand.Subscribe(channel, channels)  => subscribe(channel, channels)
      case RedisPubSubCommand.PSubscribe(pattern, patterns) => pSubscribe(pattern, patterns)
      case RedisPubSubCommand.Unsubscribe(channels)         => unsubscribe(channels)
      case RedisPubSubCommand.PUnsubscribe(patterns)        => pUnsubscribe(patterns)
    }

  private def subscribe(
    channel: String,
    channels: List[String]
  ): ZStream[BinaryCodec, RedisError, PushProtocol] =
    makeSubscriptionStream(PubSub.Subscribe, SubscriptionKey.Channel(channel), channels.map(SubscriptionKey.Channel(_)))

  private def pSubscribe(
    pattern: String,
    patterns: List[String]
  ): ZStream[BinaryCodec, RedisError, PushProtocol] =
    makeSubscriptionStream(
      PubSub.PSubscribe,
      SubscriptionKey.Pattern(pattern),
      patterns.map(SubscriptionKey.Pattern(_))
    )

  private def unsubscribe(channels: List[String]): ZStream[BinaryCodec, RedisError, PushProtocol] =
    makeUnsubscriptionStream(PubSub.Unsubscribe, channels.map(SubscriptionKey.Channel(_)))

  private def pUnsubscribe(patterns: List[String]): ZStream[BinaryCodec, RedisError, PushProtocol] =
    makeUnsubscriptionStream(PubSub.PUnsubscribe, patterns.map(SubscriptionKey.Pattern(_)))

  private def makeSubscriptionStream(command: String, key: SubscriptionKey, keys: List[SubscriptionKey]) =
    ZStream.unwrap[BinaryCodec, RedisError, PushProtocol](
      ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
        for {
          promise <- Promise.make[RedisError, Unit]
          chunk    = StringInput.encode(command) ++ NonEmptyList(StringInput).encode((key.value, keys.map(_.value)))
          stream  <- makeStream(key :: keys)
          _       <- reqQueue.offer(Request(chunk, promise))
          _       <- promise.await
        } yield stream
      }
    )

  private def makeUnsubscriptionStream[T <: SubscriptionKey: ClassTag](command: String, keys: List[T]) =
    ZStream.unwrap[BinaryCodec, RedisError, PushProtocol](
      ZIO.serviceWithZIO[BinaryCodec] { implicit codec =>
        for {
          targets <- if (keys.isEmpty) pubSubHubsRef.get.map(_.keys.collect { case t: T => t }.toList)
                     else ZIO.succeedNow(keys)
          chunk    = StringInput.encode(command) ++ Varargs(StringInput).encode(keys.map(_.value))
          promise <- Promise.make[RedisError, Unit]
          stream  <- makeStream(targets)
          _       <- reqQueue.offer(Request(chunk, promise))
          _       <- promise.await
        } yield stream
      }
    )

  private def makeStream(keys: List[SubscriptionKey]): UIO[Stream[RedisError, PushProtocol]] =
    for {
      streams <- ZIO.foreach(keys)(getHub(_).map(ZStream.fromHub(_)))
      stream   = streams.fold(ZStream.empty)(_ merge _)
    } yield stream

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
  final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, Unit])

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
