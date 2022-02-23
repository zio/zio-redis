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
import zio.{Has, URLayer, ZIO, ZLayer}

trait Redis {
  def codec: Codec
  def executor: RedisExecutor
}

object Redis {

  lazy val live: URLayer[Has[RedisExecutor] with Has[Codec], Has[Redis]] =
    ZLayer.identity[Has[Codec]] ++ ZLayer.identity[Has[RedisExecutor]] >>> RedisService

  private[this] final val RedisService: ZLayer[Has[Codec] with Has[RedisExecutor], Nothing, Has[Redis]] =
    ZIO
      .services[Codec, RedisExecutor]
      .map { env =>
        new Redis {
          val codec: Codec            = env._1
          val executor: RedisExecutor = env._2
        }
      }
      .toLayer
}
