/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.internal

import zio.redis.Input.{CommandNameInput, StringInput}
import zio.redis.Output.PushMessageOutput
import zio.redis.api.Subscription
import zio.redis.internal.PubSub.{PushMessage, SubscriptionKey}
import zio.redis.internal.SingleNodeSubscriptionExecutor.{Request, RequestQueueSize, True}
import zio.redis.{Input, RedisError}
import zio.stream._
import zio.{Chunk, ChunkBuilder, IO, Promise, Queue, Ref, Schedule, Scope, UIO, URIO, ZIO}

private[redis] final class SingleNodeSubscriptionExecutor private (
  subsRef: Ref[Map[SubscriptionKey, Chunk[Queue[Take[RedisError, PushMessage]]]]],
  requests: Queue[Request],
  connection: RedisConnection
) extends SubscriptionExecutor {
  def execute(command: RespCommand): Stream[RedisError, PushMessage] =
    ZStream
      .fromZIO(
        for {
          commandName <-
            ZIO
              .fromOption(command.args.collectFirst { case RespCommandArgument.CommandName(name) => name })
              .orElseFail(RedisError.CommandNameNotFound(command))
          stream <- commandName match {
                      case Subscription.Subscribe =>
                        ZIO.succeed(subscribe(extractKeys(command).map(SubscriptionKey.Channel(_)), command.args))
                      case Subscription.PSubscribe =>
                        ZIO.succeed(subscribe(extractKeys(command).map(SubscriptionKey.Pattern(_)), command.args))
                      case Subscription.Unsubscribe  => ZIO.succeed(unsubscribe(command.args))
                      case Subscription.PUnsubscribe => ZIO.succeed(unsubscribe(command.args))
                      case other                     => ZIO.fail(RedisError.InvalidPubSubCommand(other))
                    }
        } yield stream
      )
      .flatten

  private def extractKeys(command: RespCommand): Chunk[String] = command.args.collect {
    case key: RespCommandArgument.Key => key.value.asString
  }

  private def releaseQueue(key: SubscriptionKey, queue: Queue[Take[RedisError, PushMessage]]): UIO[Unit] =
    subsRef.update(subs =>
      subs.get(key) match {
        case Some(queues) => subs.updated(key, queues.filterNot(_ == queue))
        case None         => subs
      }
    )

  private def subscribe(
    keys: Chunk[SubscriptionKey],
    command: Chunk[RespCommandArgument]
  ): Stream[RedisError, PushMessage] =
    ZStream
      .fromZIO(
        for {
          queues <- ZIO.foreach(keys)(key =>
                      Queue
                        .unbounded[Take[RedisError, PushMessage]]
                        .tap(queue =>
                          subsRef.update(subscription =>
                            subscription.updated(
                              key,
                              subscription.getOrElse(key, Chunk.empty) ++ Chunk.single(queue)
                            )
                          )
                        )
                        .map(key -> _)
                    )
          promise <- Promise.make[RedisError, Unit]
          _ <- requests.offer(
                 Request(
                   command.map(_.value),
                   promise
                 )
               )
          _ <- promise.await.tapError(_ =>
                 ZIO.foreachDiscard(queues) { case (key, queue) =>
                   queue.shutdown *> releaseQueue(key, queue)
                 }
               )
          streams = queues.map { case (key, queue) =>
                      ZStream
                        .fromQueueWithShutdown(queue)
                        .ensuring(releaseQueue(key, queue))
                    }
        } yield streams.fold(ZStream.empty)(_ merge _)
      )
      .flatten
      .flattenTake

  private def unsubscribe(command: Chunk[RespCommandArgument]): Stream[RedisError, PushMessage] =
    ZStream
      .fromZIO(
        for {
          promise <- Promise.make[RedisError, Unit]
          _       <- requests.offer(Request(command.map(_.value), promise))
          _       <- promise.await
        } yield ZStream.empty
      )
      .flatten

  private def send =
    requests.takeBetween(1, RequestQueueSize).flatMap { reqs =>
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
    def offerMessage(msg: PushMessage) =
      for {
        subscription <- subsRef.get
        _ <- ZIO.foreachDiscard(subscription.get(msg.key))(
               ZIO.foreachDiscard(_)(queue => queue.offer(Take.single(msg)).unlessZIO(queue.isShutdown))
             )
      } yield ()

    def releaseStream(key: SubscriptionKey) =
      for {
        subscription <- subsRef.getAndUpdate(_ - key)
        _ <- ZIO.foreachDiscard(subscription.get(key))(
               ZIO.foreachDiscard(_)(queue => queue.offer(Take.end).unlessZIO(queue.isShutdown))
             )
      } yield ()

    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .mapZIO(resp => ZIO.attempt(PushMessageOutput.unsafeDecode(resp)))
      .refineToOrDie[RedisError]
      .foreach {
        case msg: PushMessage.Subscribed   => offerMessage(msg)
        case msg: PushMessage.Unsubscribed => offerMessage(msg) *> releaseStream(msg.key)
        case msg: PushMessage.Message      => offerMessage(msg)
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
      subsKeys <- subsRef.get.map(_.keys)
      (channels, patterns) = subsKeys.partition {
                               case _: SubscriptionKey.Channel => false
                               case _: SubscriptionKey.Pattern => true
                             }
      commands = makeCommand(Subscription.Subscribe, Chunk.fromIterable(channels).map(_.value)) ++
                   makeCommand(Subscription.PSubscribe, Chunk.fromIterable(patterns).map(_.value))
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

private[redis] object SingleNodeSubscriptionExecutor {
  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, Unit]
  )

  private final val True: Any => Boolean = _ => true

  private final val RequestQueueSize = 16

  def create(conn: RedisConnection): URIO[Scope, SubscriptionExecutor] =
    for {
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      subsRef  <- Ref.make(Map.empty[SubscriptionKey, Chunk[Queue[Take[RedisError, PushMessage]]]])
      pubSub    = new SingleNodeSubscriptionExecutor(subsRef, reqQueue, conn)
      _        <- pubSub.run.forkScoped
      _        <- logScopeFinalizer(s"$pubSub Subscription Node is closed")
    } yield pubSub
}
