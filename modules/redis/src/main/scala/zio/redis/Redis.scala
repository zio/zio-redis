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

object Redis {
  lazy val cluster: ZLayer[CodecSupplier & RedisClusterConfig, RedisError, Redis] =
    ZLayer.makeSome[CodecSupplier & RedisClusterConfig, Redis](ClusterExecutor.layer, makeLayer)

  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, Redis & AsyncRedis] =
    ZLayer.makeSome[CodecSupplier, Redis & AsyncRedis](
      SingleNodeExecutor.local,
      makeLayer
    )

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, Redis & AsyncRedis] =
    ZLayer.makeSome[CodecSupplier & RedisConfig, Redis & AsyncRedis](
      SingleNodeExecutor.layer,
      makeLayer
    )
  private def makeLayer
    : URLayer[CodecSupplier & RedisExecutor, GenRedis[RedisExecutor.Async] & GenRedis[RedisExecutor.Sync]] =
    ZLayer.fromZIOEnvironment {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[RedisExecutor]
      } yield ZEnvironment[GenRedis[RedisExecutor.Async], GenRedis[RedisExecutor.Sync]](
        new AsyncLive(codecSupplier, executor),
        new SyncLive(codecSupplier, executor)
      )
    }

  private final class SyncLive(val codecSupplier: CodecSupplier, val executor: RedisExecutor)
      extends GenRedis[RedisExecutor.Sync] {
    def toG[A](in: UIO[IO[RedisError, A]]): RedisExecutor.Sync[A] = RedisExecutor.sync(in)
  }

  private final class AsyncLive(val codecSupplier: CodecSupplier, val executor: RedisExecutor)
      extends GenRedis[RedisExecutor.Async] {
    def toG[A](in: UIO[IO[RedisError, A]]): RedisExecutor.Async[A] = RedisExecutor.async(in)
  }
}
