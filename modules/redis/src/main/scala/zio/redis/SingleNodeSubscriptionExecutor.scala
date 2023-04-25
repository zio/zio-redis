package zio.redis

import zio.redis.Input.{CommandNameInput, StringInput}
import zio.redis.Output.PushProtocolOutput
import zio.redis.SingleNodeSubscriptionExecutor.{Request, RequestQueueSize, True}
import zio.redis.api.Subscription
import zio.redis.internal.{RedisConnection, RespCommand, RespCommandArgument, RespValue, logScopeFinalizer}
import zio.redis.options.PubSub.PushProtocol
import zio.stream._
import zio.{Chunk, ChunkBuilder, IO, Promise, Queue, Ref, Schedule, ZIO}

final class SingleNodeSubscriptionExecutor(
  channelSubsRef: Ref[Map[String, Chunk[Queue[Take[RedisError, PushProtocol]]]]],
  patternSubsRef: Ref[Map[String, Chunk[Queue[Take[RedisError, PushProtocol]]]]],
  reqQueue: Queue[Request],
  connection: RedisConnection
) extends SubscriptionExecutor {
  def execute(command: RespCommand): Stream[RedisError, PushProtocol] =
    ZStream
      .fromZIO(
        for {
          commandName <-
            ZIO
              .fromOption(command.args.collectFirst { case RespCommandArgument.CommandName(name) => name })
              .orElseFail(RedisError.CommandNameNotFound(command.args.toString()))
          stream <- commandName match {
                      case Subscription.Subscribe    => ZIO.succeed(subscribe(channelSubsRef, command))
                      case Subscription.PSubscribe   => ZIO.succeed(subscribe(patternSubsRef, command))
                      case Subscription.Unsubscribe  => ZIO.succeed(unsubscribe(command))
                      case Subscription.PUnsubscribe => ZIO.succeed(unsubscribe(command))
                      case other                     => ZIO.fail(RedisError.InvalidPubSubCommand(other))
                    }
        } yield stream
      )
      .flatten

  private def subscribe(
    subscriptionRef: Ref[Map[String, Chunk[Queue[Take[RedisError, PushProtocol]]]]],
    command: RespCommand
  ): Stream[RedisError, PushProtocol] =
    ZStream
      .fromZIO(
        for {
          queues <- ZIO.foreach(command.args.collect { case key: RespCommandArgument.Key => key.value.asString })(key =>
                      Queue
                        .unbounded[Take[RedisError, PushProtocol]]
                        .tap(queue =>
                          subscriptionRef.update(
                            _.updatedWith(key)(_.map(_ appended queue).orElse(Some(Chunk.single(queue))))
                          )
                        )
                        .map(key -> _)
                    )
          promise <- Promise.make[RedisError, Unit]
          _ <- reqQueue.offer(
                 Request(
                   command.args.map(_.value),
                   promise
                 )
               )
          streams = queues.map { case (key, queue) =>
                      ZStream
                        .fromQueueWithShutdown(queue)
                        .ensuring(subscriptionRef.update(_.updatedWith(key)(_.map(_.filterNot(_ == queue)))))
                    }
          _ <- promise.await.tapError(_ => ZIO.foreachDiscard(queues) { case (_, queue) => queue.shutdown })
        } yield streams.fold(ZStream.empty)(_ merge _)
      )
      .flatten
      .flattenTake

  private def unsubscribe(command: RespCommand): Stream[RedisError, PushProtocol] =
    ZStream
      .fromZIO(
        for {
          promise <- Promise.make[RedisError, Unit]
          _       <- reqQueue.offer(Request(command.args.map(_.value), promise))
          _       <- promise.await
        } yield ZStream.empty
      )
      .flatten

  private def send =
    reqQueue.takeBetween(1, RequestQueueSize).flatMap { reqs =>
      val buffer = ChunkBuilder.make[Byte]()
      val it     = reqs.iterator

      while (it.hasNext) {
        val req = it.next()
        buffer ++= RespValue.Array(req.command).asBytes
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

  private def receive: IO[RedisError, Unit] = {
    def offerMessage(
      subscriptionRef: Ref[Map[String, Chunk[Queue[Take[RedisError, PushProtocol]]]]],
      key: String,
      msg: PushProtocol
    ) = for {
      subscription <- subscriptionRef.get
      _ <- ZIO.foreachDiscard(subscription.get(key))(
             ZIO.foreachDiscard(_)(queue => queue.offer(Take.single(msg)).unlessZIO(queue.isShutdown))
           )
    } yield ()

    def releaseStream(subscriptionRef: Ref[Map[String, Chunk[Queue[Take[RedisError, PushProtocol]]]]], key: String) =
      for {
        subscription <- subscriptionRef.getAndUpdate(_ - key)
        _ <- ZIO.foreachDiscard(subscription.get(key))(
               ZIO.foreachDiscard(_)(queue => queue.offer(Take.end).unlessZIO(queue.isShutdown))
             )
      } yield ()

    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp)))
      .refineToOrDie[RedisError]
      .foreach {
        case msg @ PushProtocol.Subscribe(channel, _) => offerMessage(channelSubsRef, channel, msg)
        case msg @ PushProtocol.Unsubscribe(channel, _) =>
          offerMessage(channelSubsRef, channel, msg) *> releaseStream(channelSubsRef, channel)
        case msg @ PushProtocol.Message(channel, _)    => offerMessage(channelSubsRef, channel, msg)
        case msg @ PushProtocol.PSubscribe(pattern, _) => offerMessage(patternSubsRef, pattern, msg)
        case msg @ PushProtocol.PUnsubscribe(pattern, _) =>
          offerMessage(patternSubsRef, pattern, msg) *> releaseStream(patternSubsRef, pattern)
        case msg @ PushProtocol.PMessage(pattern, _, _) => offerMessage(patternSubsRef, pattern, msg)
      }
  }

  private def resubscribe: IO[RedisError, Unit] = {
    def makeCommand(name: String, keys: Chunk[String]) =
      if (keys.isEmpty)
        Chunk.empty
      else
        RespValue
          .Array((CommandNameInput.encode(name) ++ Input.Varargs(StringInput).encode(keys)).args.map(_.value))
          .asBytes

    for {
      channels <- channelSubsRef.get.map(_.keys)
      patterns <- patternSubsRef.get.map(_.keys)
      commands = makeCommand(Subscription.Subscribe, Chunk.fromIterable(channels)) ++
                   makeCommand(Subscription.PSubscribe, Chunk.fromIterable(patterns))
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
      reqQueue   <- Queue.bounded[Request](RequestQueueSize)
      channelRef <- Ref.make(Map.empty[String, Chunk[Queue[Take[RedisError, PushProtocol]]]])
      patternRef <- Ref.make(Map.empty[String, Chunk[Queue[Take[RedisError, PushProtocol]]]])
      pubSub      = new SingleNodeSubscriptionExecutor(channelRef, patternRef, reqQueue, conn)
      _          <- pubSub.run.forkScoped
      _          <- logScopeFinalizer(s"$pubSub Subscription Node is closed")
    } yield pubSub
}
