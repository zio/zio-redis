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
import zio._

object Redis {
  lazy val cluster: ZLayer[CodecSupplier & RedisClusterConfig, RedisError, Redis] =
    ZLayer.makeSome[CodecSupplier & RedisClusterConfig, Redis](ClusterExecutor.layer, makeLayer[RedisExecutor.Sync])

  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, Redis & AsyncRedis] =
    ZLayer.makeSome[CodecSupplier, Redis & AsyncRedis](
      SingleNodeExecutor.local,
      makeLayer[RedisExecutor.Sync],
      makeLayer[RedisExecutor.Async]
    )

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, Redis & AsyncRedis] =
    SingleNodeExecutor.layer >>> (makeLayer[RedisExecutor.Sync] ++ makeLayer[RedisExecutor.Async])

  private def makeLayer[G[+_]: TagK]: URLayer[CodecSupplier & RedisExecutor[G], GenRedis[G]] =
    ZLayer {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[RedisExecutor[G]]
      } yield new Live(codecSupplier, executor)
    }

  private final class Live[G[+_]](val codecSupplier: CodecSupplier, val executor: RedisExecutor[G]) extends GenRedis[G]
}
