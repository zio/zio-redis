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

package zio.redis.api

import zio.redis.ResultBuilder.ResultStreamBuilder1
import zio.redis._
import zio.redis.api.Subscription.{NoopCallback, PubSubCallback}
import zio.redis.internal._
import zio.schema.Schema
import zio.stream.Stream
import zio.{Chunk, IO, UIO, ZIO}

trait Subscription extends SubscribeEnvironment {

  /**
   * Subscribes the client to the specified channel.
   *
   * @param channel
   *   Channel name to subscribe to.
   * @return
   *   Returns stream that only emits published value for the given channel.
   */
  final def subscribeSingle(channel: String): ResultStreamBuilder1[Id] =
    subscribeSingleWith(channel)()

  /**
   * Subscribes the client to the specified channel with callbacks for Subscribe and Unsubscribe messages.
   *
   * @param channel
   *   Channel name to subscribe to.
   * @param onSubscribe
   *   Callback for given channel name and the number of subscribers of channel from upstream when subscribed.
   * @param onUnsubscribe
   *   Callback for given channel name and the number of subscribers of channel from upstream when unsubscribed.
   * @return
   */
  final def subscribeSingleWith(channel: String)(
    onSubscribe: PubSubCallback = NoopCallback,
    onUnsubscribe: PubSubCallback = NoopCallback
  ): ResultStreamBuilder1[Id] =
    new ResultStreamBuilder1[Id] {
      def returning[R: Schema]: Stream[RedisError, R] =
        RedisSubscriptionCommand(executor)
          .subscribe(
            Chunk.single(channel),
            onSubscribe,
            onUnsubscribe
          )
          .map(_._2)
    }

  /**
   * Subscribes the client to the specified channels.
   * @param channel
   *   Channel name to subscribe to.
   * @param channels
   *   Channel names to subscribe to consecutively.
   * @return
   *   Returns stream that emits published value keyed by that channel name for the given channels.
   */
  final def subscribe(
    channel: String,
    channels: String*
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    subscribeWith(channel, channels: _*)(NoopCallback, NoopCallback)

  /**
   * Subscribes the client to the specified channels with callbacks for Subscribe and Unsubscribe messages.
   * @param channel
   *   Channel name to subscribe to.
   * @param channels
   *   Channel names to subscribe to consecutively.
   * @param onSubscribe
   *   Callback for given channel name and the number of subscribers of channel from upstream when subscribed.
   * @param onUnsubscribe
   *   Callback for given channel name and the number of subscribers of channel from upstream when unsubscribed.
   * @return
   *   Returns stream that emits published value keyed by that channel name for the given channels.
   */
  final def subscribeWith(channel: String, channels: String*)(
    onSubscribe: PubSubCallback = NoopCallback,
    onUnsubscribe: PubSubCallback = NoopCallback
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    new ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] {
      def returning[R: Schema]: Stream[RedisError, (String, R)] =
        RedisSubscriptionCommand(executor).subscribe(
          Chunk.fromIterable(channel +: channels),
          onSubscribe,
          onUnsubscribe
        )
    }

  /**
   * Subscribe to messages published to all channels that match the given patterns.
   *
   * @param pattern
   *   Pattern to subscribing to matching channels. (for the more details about pattern format, see
   *   https://redis.io/commands/psubscribe)
   * @param patterns
   *   Patterns to subscribing to matching channels consecutively.
   * @return
   *   Returns stream that emits published value keyed by the matching channel name.
   */
  final def pSubscribe(
    pattern: String,
    patterns: String*
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    pSubscribeWith(pattern, patterns: _*)(NoopCallback, NoopCallback)

  /**
   * Subscribe to messages published to all channels that match the given pattern with callbacks for PSubscribe and
   * PUnsubscribe messages.
   * @param pattern
   *   Pattern to subscribing to matching channels. (for the more details about pattern format, see
   *   https://redis.io/commands/psubscribe)
   * @param patterns
   *   Patterns to subscribing to matching channels consecutively.
   * @param onSubscribe
   *   Callback for given pattern and the number of subscribers of pattern from upstream when subscribed.
   * @param onUnsubscribe
   *   Callback for given pattern and the number of subscribers of pattern from upstream when unsubscribed.
   * @return
   */
  final def pSubscribeWith(pattern: String, patterns: String*)(
    onSubscribe: PubSubCallback = NoopCallback,
    onUnsubscribe: PubSubCallback = NoopCallback
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    new ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] {
      def returning[R: Schema]: Stream[RedisError, (String, R)] =
        RedisSubscriptionCommand(executor).pSubscribe(
          Chunk.fromIterable(pattern +: patterns),
          onSubscribe,
          onUnsubscribe
        )
    }

  /**
   * Unsubscribes the client from the given channels, or from all of them if none is given.
   * @param channels
   *   Channels to unsubscribe from. if this is empty, all channels that this client is subscribed to will be
   *   unsubscribed.
   * @return
   *   Returns unit if successful, error otherwise.
   */
  final def unsubscribe(channels: String*): IO[RedisError, Unit] =
    RedisSubscriptionCommand(executor).unsubscribe(Chunk.fromIterable(channels))

  /**
   * Unsubscribes the client from the given patterns, or from all of them if none is given.
   * @param patterns
   *   Patterns to unsubscribe from. if this is empty, all patterns that this client is subscribed to will be
   *   unsubscribed.
   * @return
   *   Returns unit if successful, error otherwise.
   */
  final def pUnsubscribe(patterns: String*): IO[RedisError, Unit] =
    RedisSubscriptionCommand(executor).pUnsubscribe(Chunk.fromIterable(patterns))

}

object Subscription {
  type PubSubCallback = (String, Long) => UIO[Unit]

  final val Subscribe    = "SUBSCRIBE"
  final val Unsubscribe  = "UNSUBSCRIBE"
  final val PSubscribe   = "PSUBSCRIBE"
  final val PUnsubscribe = "PUNSUBSCRIBE"

  private final val NoopCallback: PubSubCallback = (_, _) => ZIO.unit
}
