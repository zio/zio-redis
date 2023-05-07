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

package zio.redis

import zio.redis.internal.{RedisConnection, RespCommand}
import zio.redis.options.PubSub.PushProtocol
import zio.stream._
import zio.{Layer, ZIO, ZLayer}

trait SubscriptionExecutor {
  private[redis] def execute(command: RespCommand): Stream[RedisError, PushProtocol]
}

object SubscriptionExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, SubscriptionExecutor] =
    RedisConnection.layer.fresh >>> pubSublayer

  lazy val local: Layer[RedisError.IOError, SubscriptionExecutor] =
    RedisConnection.local.fresh >>> pubSublayer

  private lazy val pubSublayer: ZLayer[RedisConnection, RedisError.IOError, SubscriptionExecutor] =
    ZLayer.scoped(
      for {
        conn   <- ZIO.service[RedisConnection]
        pubSub <- SingleNodeSubscriptionExecutor.create(conn)
      } yield pubSub
    )
}
