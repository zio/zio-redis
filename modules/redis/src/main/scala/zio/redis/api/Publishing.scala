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
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.schema.Schema
import zio.{Chunk, IO}

trait Publishing extends RedisEnvironment {
  import Publishing._

  final def publish[A: Schema](channel: String, message: A): IO[RedisError, Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryKeyInput[A]()), LongOutput, executor)
    command.run((channel, message))
  }

  final def pubSubChannels(pattern: String): IO[RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, StringInput, ChunkOutput(MultiStringOutput), executor)
    command.run(pattern)
  }

  final def pubSubNumPat: IO[RedisError, Long] = {
    val command = RedisCommand(PubSubNumPat, NoInput, LongOutput, executor)
    command.run(())
  }

  final def pubSubNumSub(channel: String, channels: String*): IO[RedisError, Chunk[NumberOfSubscribers]] = {
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
