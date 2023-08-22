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

import zio._
import zio.concurrent.ConcurrentMap
import zio.redis.Input.{CommandNameInput, StringInput}
import zio.redis.Output.PushMessageOutput
import zio.redis.api.Subscription
import zio.redis.internal.PubSub.{PushMessage, SubscriptionKey}
import zio.redis.internal.SingleNodeRunner.True
import zio.redis.internal.SingleNodeSubscriptionExecutor.Request
import zio.redis.{Input, RedisError}
import zio.stream._

private[redis] final class SingleNodeSubscriptionExecutor private (
  subscriptionMap: ConcurrentMap[SubscriptionKey, Hub[Take[RedisError, PushMessage]]],
  requests: Queue[Request],
  subsResponses: Queue[Promise[RedisError, PushMessage]],
  connection: RedisConnection
) extends SingleNodeRunner
    with SubscriptionExecutor {
  def execute(command: RespCommand): Stream[RedisError, PushMessage] = {
    def getStream(commandName: String): IO[RedisError, Stream[RedisError, PushMessage]] = commandName match {
      case Subscription.Subscribe    =>
        subscribe(extractChannelKeys(command.args), command)
      case Subscription.PSubscribe   =>
        subscribe(extractPatternKeys(command.args), command)
      case Subscription.Unsubscribe  =>
        unsubscribe(command).as(ZStream.empty)
      case Subscription.PUnsubscribe =>
        unsubscribe(command).as(ZStream.empty)
      case other                     => ZIO.fail(RedisError.InvalidPubSubCommand(other))
    }

    for {
      commandName <- ZStream.fromZIO(
                       ZIO
                         .fromOption(command.args.collectFirst { case RespCommandArgument.CommandName(name) =>
                           name
                         })
                         .orElseFail(RedisError.CommandNameNotFound(command))
                     )
      pushMessage <- ZStream.fromZIO(getStream(commandName)).flatten
    } yield pushMessage
  }

  private def subscribe(
    keys: Chunk[SubscriptionKey],
    command: RespCommand
  ): IO[RedisError, Stream[RedisError, PushMessage]] = {
    def getHub(key: SubscriptionKey): IO[RedisError, Hub[Take[RedisError, PushMessage]]] =
      subscriptionMap
        .get(key)
        .flatMap(ZIO.fromOption(_))
        .orElseFail(RedisError.SubscriptionStreamAlreadyClosed(key))

    def getStream(promise: Promise[RedisError, PushMessage]) =
      for {
        subscribed <- ZStream.fromZIO(promise.await)
        head        = ZStream(subscribed)
        tail        = ZStream.fromZIO(getHub(subscribed.key)).flatMap(ZStream.fromHub(_)).flattenTake
        message    <- head ++ tail
      } yield message

    def makeHub(key: SubscriptionKey): UIO[Unit] =
      Hub
        .unbounded[Take[RedisError, PushMessage]]
        .tap(subscriptionMap.putIfAbsent(key, _))
        .unit

    for {
      _        <- ZIO.foreachDiscard(keys)(makeHub)
      promises <- Promise.make[RedisError, PushMessage].replicateZIO(keys.size).map(Chunk.fromIterable)
      _        <- requests.offer(Request.Subscribe(command.args.map(_.value), promises))
      streams   = promises.map(getStream)
    } yield streams.fold(ZStream.empty)(_ merge _)
  }

  private def unsubscribe(command: RespCommand): IO[RedisError, Unit] =
    for {
      promise <- Promise.make[RedisError, Unit]
      _       <- requests.offer(Request.Unsubscribe(command.args.map(_.value), promise))
      _       <- promise.await
    } yield ()

  private def extractChannelKeys(command: Chunk[RespCommandArgument]): Chunk[SubscriptionKey.Channel] =
    command.collect { case RespCommandArgument.Key(key) =>
      key.asString
    }.map(SubscriptionKey.Channel.apply)

  private def extractPatternKeys(command: Chunk[RespCommandArgument]): Chunk[SubscriptionKey.Pattern] =
    command.collect { case RespCommandArgument.Key(key) =>
      key.asString
    }.map(SubscriptionKey.Pattern.apply)

  def send: IO[RedisError.IOError, Unit] =
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
        .mapError(RedisError.IOError.apply)
        .tapBoth(
          e =>
            ZIO.foreachDiscard(reqs) {
              case Request.Subscribe(_, promises)  => ZIO.foreachDiscard(promises)(_.fail(e))
              case Request.Unsubscribe(_, promise) => promise.fail(e)
            },
          _ =>
            ZIO.foreachDiscard(reqs) {
              case Request.Subscribe(_, promises)  => subsResponses.offerAll(promises)
              case Request.Unsubscribe(_, promise) => promise.succeed(())
            }
        )
        .unit
    }

  def receive: IO[RedisError, Unit] = {
    def offerMessage(msg: PushMessage) =
      for {
        hub <- subscriptionMap.get(msg.key)
        _   <- ZIO.foreachDiscard(hub)(_.offer(Take.single(msg)))
      } yield ()

    def releasePromise(msg: PushMessage): UIO[Unit] =
      for {
        promise <- subsResponses.take
        _       <- promise.succeed(msg)
      } yield ()

    def releaseHub(key: SubscriptionKey): UIO[Unit] =
      for {
        hub <- subscriptionMap.remove(key)
        _   <- ZIO.foreachDiscard(hub)(_.offer(Take.end))
      } yield ()

    connection.read
      .mapError(RedisError.IOError.apply)
      .via(RespValue.Decoder)
      .collectSome
      .mapZIO(resp => ZIO.attempt(PushMessageOutput.unsafeDecode(resp)))
      .refineToOrDie[RedisError]
      .foreach {
        case msg: PushMessage.Subscribed   => releasePromise(msg)
        case msg: PushMessage.Unsubscribed => offerMessage(msg) *> releaseHub(msg.key)
        case msg: PushMessage.Message      => offerMessage(msg)
      }
  }

  def onError(e: RedisError): IO[RedisError, Unit] = {
    def makeCommand(name: String, keys: Chunk[String]) =
      if (keys.isEmpty)
        Chunk.empty
      else
        RespValue
          .Array((CommandNameInput.encode(name) ++ Input.Varargs(StringInput).encode(keys)).args.map(_.value))
          .asBytes

    for {
      _                   <- subsResponses.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))
      subscriptions       <- subscriptionMap.toChunk
      (channels, patterns) = subscriptions.map(_._1).partition {
                               case _: SubscriptionKey.Channel => false
                               case _: SubscriptionKey.Pattern => true
                             }
      commands             = makeCommand(Subscription.Subscribe, Chunk.fromIterable(channels).map(_.value)) ++
                               makeCommand(Subscription.PSubscribe, Chunk.fromIterable(patterns).map(_.value))
      _                   <- connection
                               .write(commands)
                               .when(commands.nonEmpty)
                               .mapError(RedisError.IOError.apply)
                               .retryWhile(True)
    } yield ()
  }
}

private[redis] object SingleNodeSubscriptionExecutor {
  private sealed trait Request {
    def command: Chunk[RespValue.BulkString]
  }

  private object Request {
    final case class Subscribe(command: Chunk[RespValue.BulkString], promises: Chunk[Promise[RedisError, PushMessage]])
        extends Request
    final case class Unsubscribe(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, Unit])
        extends Request
  }

  def create(conn: RedisConnection): URIO[Scope, SubscriptionExecutor] =
    for {
      reqQueue <- Queue.bounded[Request](RequestQueueSize)
      resQueue <- Queue.unbounded[Promise[RedisError, PushMessage]]
      subsRef  <- ConcurrentMap.empty[SubscriptionKey, Hub[Take[RedisError, PushMessage]]]
      pubSub    = new SingleNodeSubscriptionExecutor(subsRef, reqQueue, resQueue, conn)
      _        <- pubSub.run.forkScoped
      _        <- logScopeFinalizer(s"$pubSub Subscription Node is closed")
    } yield pubSub
}
