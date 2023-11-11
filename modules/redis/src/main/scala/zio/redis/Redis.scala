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
import zio.redis.internal._

trait GenRedis[G[+_]]
    extends api.Connection[G]
    with api.Geo[G]
    with api.Hashes[G]
    with api.HyperLogLog[G]
    with api.Keys[G]
    with api.Lists[G]
    with api.Sets[G]
    with api.Strings[G]
    with api.SortedSets[G]
    with api.Streams[G]
    with api.Scripting[G]
    with api.Cluster[G]
    with api.Publishing[G]

trait Redis extends GenRedis[RedisExecutor.Sync]
trait AsyncRedis extends GenRedis[RedisExecutor.Async]

object Redis {
  lazy val cluster: ZLayer[CodecSupplier & RedisClusterConfig, RedisError, Redis] =
    ClusterExecutor.layer >>> makeLayer

  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, Redis] =
    SingleNodeExecutor.local >>> makeLayer

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, Redis] =
    SingleNodeExecutor.layer >>> makeLayer

  private def makeLayer: URLayer[CodecSupplier & RedisExecutor[RedisExecutor.Sync], Redis] =
    ZLayer {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[RedisExecutor[RedisExecutor.Sync]]
      } yield new Live(codecSupplier, executor)
    }

  private final class Live(val codecSupplier: CodecSupplier, val executor: RedisExecutor[RedisExecutor.Sync]) extends Redis
}
