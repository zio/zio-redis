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
import zio.schema.codec.BinaryCodec

trait Redis
    extends api.Connection
    with api.Geo
    with api.Hashes
    with api.HyperLogLog
    with api.Keys
    with api.Lists
    with api.Sets
    with api.Strings
    with api.SortedSets
    with api.Streams
    with api.Scripting
    with api.Cluster {
  protected def codec: BinaryCodec
  protected def executor: RedisExecutor
}

object Redis {
  lazy val layer: URLayer[RedisExecutor with BinaryCodec, Redis] =
    ZLayer {
      for {
        executor <- ZIO.service[RedisExecutor]
        codec    <- ZIO.service[BinaryCodec]
      } yield Live(codec, executor)
    }

  private final case class Live(codec: BinaryCodec, executor: RedisExecutor) extends Redis
}
