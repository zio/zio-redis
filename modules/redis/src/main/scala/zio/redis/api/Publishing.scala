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

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.schema.Schema
import zio.Chunk

trait Publishing[G[+_]] extends RedisEnvironment[G] {
  import Publishing._

  /**
   * Posts a message to the given channel.
   *
   * @param channel
   *   Target channel name for publishing messages.
   * @param message
   *   The value of the message to be published to the channel.
   * @return
   *   Returns the number of clients that received the message.
   */
  final def publish[A: Schema](channel: String, message: A): G[Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryKeyInput[A]()), LongOutput, executor)
    command.run((channel, message))
  }

  /**
   * Lists the currently active channel that has one or more subscribers (excluding clients subscribed to patterns).
   *
   * @param pattern
   *   Pattern to get matching channels.
   * @return
   *   Returns a list of active channels matching the specified pattern.
   */
  final def pubSubChannels(pattern: String): G[Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, StringInput, ChunkOutput(MultiStringOutput), executor)
    command.run(pattern)
  }

  /**
   * The number of unique patterns that are subscribed to by clients.
   *
   * @return
   *   Returns the number of patterns all the clients are subscribed to.
   */
  final def pubSubNumPat: G[Long] = {
    val command = RedisCommand(PubSubNumPat, NoInput, LongOutput, executor)
    command.run(())
  }

  /**
   * The number of subscribers (exclusive of clients subscribed to patterns) for the specified channels.
   *
   * @param channel
   *   Channel name to get the number of subscribers.
   * @param channels
   *   Channel names to get the number of subscribers.
   * @return
   *   Returns a map of channel and number of subscribers for channel.
   */
  final def pubSubNumSub(channel: String, channels: String*): G[Map[String, Long]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput, executor)
    command.run((channel, channels.toList))
  }
}

private[redis] object Publishing {
  final val Publish        = "PUBLISH"
  final val PubSubChannels = "PUBSUB CHANNELS"
  final val PubSubNumPat   = "PUBSUB NUMPAT"
  final val PubSubNumSub   = "PUBSUB NUMSUB"
}
