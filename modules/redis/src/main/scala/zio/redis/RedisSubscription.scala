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

import zio._

trait RedisSubscription extends api.Subscription

object RedisSubscription {
  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, RedisSubscription] =
    SubscriptionExecutor.local >>> makeLayer

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, RedisSubscription] =
    SubscriptionExecutor.layer >>> makeLayer

  private def makeLayer: URLayer[CodecSupplier & SubscriptionExecutor, RedisSubscription] =
    ZLayer.fromFunction(Live.apply _)

  private final case class Live(codecSupplier: CodecSupplier, executor: SubscriptionExecutor) extends RedisSubscription
}
