package zio.redis

import zio.redis.SingleNodeSubscriptionExecutor.{Request, RequestQueueSize, True}
import zio.redis.api.Subscribe
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, IO, Promise, Queue, Ref, Schedule, ZIO}

final class SingleNodeSubscriptionExecutor(
  channelSubsRef: Ref[Set[RespArgument.Key]],
  patternSubsRef: Ref[Set[RespArgument.Key]],
  hub: Hub[RespValue],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends SubscriptionExecutor {
  def execute(command: RespCommand): IO[RedisError, Stream[RedisError, RespValue]] =
    for {
      commandName <- ZIO
                       .fromOption(command.args.collectFirst { case RespArgument.CommandName(name) => name })
                       .orElseFail(RedisError.CommandNameNotFound(command.args.toString()))
      stream <- commandName match {
                  case Subscribe.Subscribe    => ZIO.succeed(subscribe(channelSubsRef, command))
                  case Subscribe.PSubscribe   => ZIO.succeed(subscribe(patternSubsRef, command))
                  case Subscribe.Unsubscribe  => ZIO.succeed(unsubscribe(channelSubsRef, command))
                  case Subscribe.PUnsubscribe => ZIO.succeed(unsubscribe(patternSubsRef, command))
                  case other               => ZIO.fail(RedisError.InvalidPubSubCommand(other))
                }
    } yield stream

  private def subscribe(
    subscriptionRef: Ref[Set[RespArgument.Key]],
    command: RespCommand
  ): Stream[RedisError, RespValue] =
    ZStream
      .fromZIO(
        for {
          reqPromise <- Promise.make[RedisError, Unit]
          _          <- reqQueue.offer(Request(command.args.map(_.value), reqPromise))
          _          <- reqPromise.await
          keys        = command.args.collect { case key: RespArgument.Key => key }
          _          <- subscriptionRef.update(_ ++ keys)
        } yield ZStream.fromHub(hub)
      )
      .flatten

  private def unsubscribe(
    subscriptionRef: Ref[Set[RespArgument.Key]],
    command: RespCommand
  ): Stream[RedisError, RespValue] =
    ZStream
      .fromZIO(
        for {
          reqPromise <- Promise.make[RedisError, Unit]
          _          <- reqQueue.offer(Request(command.args.map(_.value), reqPromise))
          _          <- reqPromise.await
          keys        = command.args.collect { case key: RespArgument.Key => key }
          _ <- subscriptionRef.update(subscribedSet =>
                 if (keys.nonEmpty)
                   subscribedSet -- keys
                 else
                   Set.empty
               )
        } yield ZStream.fromHub(hub)
      )
      .flatten

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
          e => ZIO.foreachDiscard(reqs.map(_.promise))(_.fail(e)),
          _ => ZIO.foreachDiscard(reqs.map(_.promise))(_.succeed(()))
        )
    }

  private def receive: IO[RedisError, Unit] =
    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.decoder)
      .collectSome
      .foreach(hub.offer(_))

  private def resubscribe: IO[RedisError, Unit] = {
    def makeCommand(name: String, keys: Chunk[RespArgument.Key]) =
      if (keys.isEmpty)
        Chunk.empty
      else
        RespValue.Array((RespCommand(RespArgument.CommandName(name)) ++ RespCommand(keys)).args.map(_.value)).serialize

    for {
      channels <- channelSubsRef.get
      patterns <- patternSubsRef.get
      commands = makeCommand(Subscribe.Subscribe, Chunk.fromIterable(channels)) ++
                   makeCommand(Subscribe.PSubscribe, Chunk.fromIterable(patterns))
      _ <- connection
             .write(commands)
             .when(commands.nonEmpty)
             .mapError(RedisError.IOError(_))
             .retryWhile(True)
    } yield ()
  }

  /**
   * Opens a connection to the server and launches receive operations. All failures are retried by opening a new
   * connection. Only exits by interruption or defect.
   */
  val run: IO[RedisError, AnyVal] =
    ZIO.logTrace(s"$this PubSub sender and reader has been started") *>
      (send.repeat(Schedule.forever) race receive)
        .tapError(e => ZIO.logWarning(s"Reconnecting due to error: $e") *> resubscribe)
        .retryWhile(True)
        .tapError(e => ZIO.logError(s"Executor exiting: $e"))
}

object SingleNodeSubscriptionExecutor {
  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, Unit]
  )

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection) =
    for {
      hub        <- Hub.unbounded[RespValue]
      reqQueue   <- Queue.bounded[Request](RequestQueueSize)
      channelRef <- Ref.make(Set.empty[RespArgument.Key])
      patternRef <- Ref.make(Set.empty[RespArgument.Key])
      pubSub      = new SingleNodeSubscriptionExecutor(channelRef, patternRef, hub, reqQueue, conn)
      _          <- pubSub.run.forkScoped
      _          <- logScopeFinalizer(s"$pubSub Node PubSub is closed")
    } yield pubSub
}
