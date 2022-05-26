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

import zio.schema.codec.Codec
import zio.{URLayer, ZIO, ZLayer}

trait Redis {
  def codec: Codec
  def executor: RedisExecutor
}

object Redis {

  lazy val live: URLayer[RedisExecutor with Codec, Redis] =
    ZLayer.service[Codec] ++ ZLayer.service[RedisExecutor] >>> RedisService

  private[this] final val RedisService: ZLayer[Codec with RedisExecutor, Nothing, Redis] =
    ZLayer.scoped(for {
      cc            <- ZIO.service[Codec]
      redisExecutor <- ZIO.service[RedisExecutor]
    } yield new Redis {
      val codec: Codec            = cc
      val executor: RedisExecutor = redisExecutor
    })
}
