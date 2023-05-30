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
import zio.redis.internal.SingleNodeRunner.True
import zio.redis.internal.SingleNodeSubscriptionExecutor.Request
import zio.redis.{Input, RedisError}
import zio.stream._
import zio.{Chunk, ChunkBuilder, Hub, IO, Promise, Queue, Ref, Scope, UIO, URIO, ZIO}

private[redis] final class SingleNodeSubscriptionExecutor private (
  subsRef: Ref[Map[SubscriptionKey, Hub[Take[RedisError, PushMessage]]]],
  requests: Queue[Request],
  responses: Queue[Promise[RedisError, PushMessage]],
  connection: RedisConnection
) extends SingleNodeRunner
    with SubscriptionExecutor {
  def execute(command: RespCommand): Stream[RedisError, PushMessage] = {
    def getStream(commandName: String): IO[RedisError, Stream[RedisError, PushMessage]] = commandName match {
      case Subscription.Subscribe =>
        subscribe(extractChannelKeys(command.args), command)
      case Subscription.PSubscribe =>
        subscribe(extractPatternKeys(command.args), command)
      case Subscription.Unsubscribe =>
        unsubscribe(extractChannelKeys(command.args), command)
      case Subscription.PUnsubscribe =>
        unsubscribe(extractPatternKeys(command.args), command)
      case other => ZIO.fail(RedisError.InvalidPubSubCommand(other))
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

  private def unsubscribe[T <: SubscriptionKey](
    keys: Chunk[T],
    command: RespCommand
  ): UIO[Stream[RedisError, PushMessage]] =
    for {
      targetKeys <- if (keys.isEmpty)
                      subsRef.get
                        .map(_.keys.collect { case key: SubscriptionKey.Pattern => key })
                        .map(Chunk.fromIterable(_))
                    else
                      ZIO.succeed(keys)
      keyPromisePairs <- ZIO.foreach(targetKeys)(key => Promise.make[RedisError, PushMessage].map(key -> _))
      _               <- requests.offer(Request(command.args.map(_.value), keyPromisePairs.map(_._2)))
      streams = keyPromisePairs.map { case (key, promise) =>
                  ZStream.fromZIO(promise.await <* subsRef.update(_ - key))
                }
    } yield streams.fold(ZStream.empty)(_ merge _)

  private def extractChannelKeys(command: Chunk[RespCommandArgument]): Chunk[SubscriptionKey.Channel] =
    command.collect { case RespCommandArgument.Key(key) =>
      key.asString
    }.map(SubscriptionKey.Channel(_))

  private def extractPatternKeys(command: Chunk[RespCommandArgument]): Chunk[SubscriptionKey.Pattern] =
    command.collect { case RespCommandArgument.Key(key) =>
      key.asString
    }.map(SubscriptionKey.Pattern(_))

  private def subscribe(
    keys: Chunk[SubscriptionKey],
    command: RespCommand
  ): UIO[Stream[RedisError, PushMessage]] = {
    def getHub(key: SubscriptionKey): UIO[Hub[Take[RedisError, PushMessage]]] =
      subsRef.get
        .map(_.get(key))
        .someOrElseZIO(
          Hub.unbounded[Take[RedisError, PushMessage]].tap(hub => subsRef.update(_ + (key -> hub)))
        )

    for {
      keyPromisePairs <- ZIO.foreach(keys)(key => Promise.make[RedisError, PushMessage].map(key -> _))
      _               <- requests.offer(Request(command.args.map(_.value), keyPromisePairs.map(_._2)))
      streams = keyPromisePairs.map { case (key, promise) =>
                  ZStream.fromZIO(promise.await) ++
                    ZStream
                      .fromZIO(getHub(key))
                      .flatMap(ZStream.fromHub(_))
                      .flattenTake
                }
    } yield streams.fold(ZStream.empty)(_ merge _)
  }

  def send: IO[RedisError.IOError, Unit] =
    requests.takeAll.flatMap { reqs =>
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
          e => ZIO.foreachDiscard(reqs.flatMap(_.promises))(_.fail(e)),
          _ => ZIO.foreachDiscard(reqs)(req => responses.offerAll(req.promises))
        )
        .unit
    }

  def receive: IO[RedisError, Unit] = {
    def offerMessage(msg: PushMessage) =
      for {
        subscription <- subsRef.get
        _            <- ZIO.foreachDiscard(subscription.get(msg.key))(_.offer(Take.single(msg)))
      } yield ()

    def releaseStream(key: SubscriptionKey) =
      for {
        subscription <- subsRef.getAndUpdate(_ - key)
        _            <- ZIO.foreachDiscard(subscription.get(key))(_.offer(Take.end))
        _            <- subsRef.update(_ - key)
      } yield ()

    def releasePromise(msg: PushMessage): UIO[Unit] =
      responses.take.flatMap(_.succeed(msg)).unit

    connection.read
      .mapError(RedisError.IOError(_))
      .via(RespValue.Decoder)
      .collectSome
      .mapZIO(resp => ZIO.attempt(PushMessageOutput.unsafeDecode(resp)))
      .refineToOrDie[RedisError]
      .foreach {
        case msg: PushMessage.Subscribed   => releasePromise(msg)
        case msg: PushMessage.Unsubscribed => releasePromise(msg) *> releaseStream(msg.key)
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
      _        <- responses.takeAll.flatMap(ZIO.foreachDiscard(_)(_.fail(e)))
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
}

private[redis] object SingleNodeSubscriptionExecutor {
  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promises: Chunk[Promise[RedisError, PushMessage]]
  )

  def create(conn: RedisConnection): URIO[Scope, SubscriptionExecutor] =
    for {
      reqQueue <- RequestQueue.create[Request]
      resQueue <- Queue.unbounded[Promise[RedisError, PushMessage]]
      subsRef  <- Ref.make(Map.empty[SubscriptionKey, Hub[Take[RedisError, PushMessage]]])
      pubSub    = new SingleNodeSubscriptionExecutor(subsRef, reqQueue, resQueue, conn)
      _        <- pubSub.run.forkScoped
      _        <- logScopeFinalizer(s"$pubSub Subscription Node is closed")
    } yield pubSub
}
