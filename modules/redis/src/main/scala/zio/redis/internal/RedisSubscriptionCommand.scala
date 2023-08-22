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

import zio.redis.Input._
import zio.redis.Output.ArbitraryOutput
import zio.redis._
import zio.redis.api.Subscription
import zio.redis.api.Subscription.PubSubCallback
import zio.redis.internal.PubSub.PushMessage._
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, IO, ZIO}

private[redis] final case class RedisSubscriptionCommand(executor: SubscriptionExecutor) {
  def subscribe[A: BinaryCodec](
    channels: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit codec: BinaryCodec[String]): Stream[RedisError, (String, A)] =
    executeCommand[A](makeCommand(Subscription.Subscribe, channels), onSubscribe, onUnsubscribe)

  def pSubscribe[A: BinaryCodec](
    patterns: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit stringCodec: BinaryCodec[String]): Stream[RedisError, (String, A)] =
    executeCommand[A](makeCommand(Subscription.PSubscribe, patterns), onSubscribe, onUnsubscribe)

  def unsubscribe(channels: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor.execute(makeCommand(Subscription.Unsubscribe, channels)).runDrain

  def pUnsubscribe(patterns: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor.execute(makeCommand(Subscription.PUnsubscribe, patterns)).runDrain

  private def makeCommand(commandName: String, keys: Chunk[String])(implicit codec: BinaryCodec[String]) =
    CommandNameInput.encode(commandName) ++
      Varargs(ArbitraryKeyInput[String]()).encode(keys)

  private def executeCommand[A: BinaryCodec](
    command: RespCommand,
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ): Stream[RedisError, (String, A)] =
    executor
      .execute(command)
      .mapZIO {
        case Subscribed(key, numOfSubs)   =>
          onSubscribe(key.value, numOfSubs).as(None)
        case Unsubscribed(key, numOfSubs) =>
          onUnsubscribe(key.value, numOfSubs).as(None)
        case Message(_, channel, message) =>
          ZIO
            .attempt(ArbitraryOutput[A]().unsafeDecode(message))
            .map(msg => Some((channel, msg)))
      }
      .collectSome
      .refineToOrDie[RedisError]
}
