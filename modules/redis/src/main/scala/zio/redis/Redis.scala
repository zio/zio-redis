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

import zio.redis.internal._
import zio.{Tag, _}

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

object Redis {
  lazy val cluster: ZLayer[CodecSupplier & RedisClusterConfig, RedisError, Redis] =
    ClusterExecutor.layer >>> makeLayer[RedisExecutor.Sync]

  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, Redis & AsyncRedis] =
    SingleNodeExecutor.local >>> (makeLayer[RedisExecutor.Sync] ++ makeLayer[RedisExecutor.Async])

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, Redis & AsyncRedis] =
    SingleNodeExecutor.layer >>> (makeLayer[RedisExecutor.Sync] ++ makeLayer[RedisExecutor.Async])

  private def makeLayer[G[+_]](implicit
    tag1: Tag[RedisExecutor[G]],
    tag2: Tag[GenRedis[G]]
  ): URLayer[CodecSupplier & RedisExecutor[G], GenRedis[G]] =
    ZLayer {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[RedisExecutor[G]]
      } yield (new Live(codecSupplier, executor): GenRedis[G])
    }

  private final class Live[G[+_]](val codecSupplier: CodecSupplier, val executor: RedisExecutor[G]) extends GenRedis[G]
}
