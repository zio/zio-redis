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

package zio.redis.transactional

import zio.redis.RedisExecutor
import zio.redis.commands.Transactions
import zio.schema.codec.BinaryCodec
import zio.{URLayer, ZLayer}

trait Redis extends api.Sets with Transactions

object Redis {
  lazy val layer: URLayer[RedisExecutor with BinaryCodec, Redis] =
    ZLayer.fromFunction(Live.apply _)

  private final case class Live(codec: BinaryCodec, executor: RedisExecutor) extends Redis
}
