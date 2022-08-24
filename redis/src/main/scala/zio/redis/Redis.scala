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
import zio.schema.codec.Codec

trait Redis {
  def codec: Codec
  def executor: RedisExecutor
}

case class RedisLive(cc: Codec, et: RedisExecutor) extends Redis {
  override def codec: Codec            = cc
  override def executor: RedisExecutor = et
}

object Redis {
  lazy val live: URLayer[RedisExecutor with Codec, Redis] = ZLayer(for {
    redisExecutor <- ZIO.service[RedisExecutor]
    cc            <- ZIO.service[Codec]
  } yield RedisLive(cc, redisExecutor))
}