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

/**
 * Builds the Redis clients. Each layer needs a `CodecSupplier`, and also a `RedisConfig` or a `RedisClusterConfig`.
 * `local` and `singleNode` provide `Redis` and `AsyncRedis`; `cluster` provides `Redis` only.
 *
 * {{{
 * import zio._
 *
 * val layer: ZLayer[Any, RedisError.IOError, Redis & AsyncRedis] =
 *   ZLayer.succeed(CodecSupplier.utf8) ++ ZLayer.succeed(RedisConfig("localhost", 6379)) >>> Redis.singleNode
 * }}}
 *
 * @see [[https://zio.dev/zio-redis/ ZIO Redis]]
 */
object Redis {
  /**
   * A client for a Redis cluster, built from a `RedisClusterConfig`. It provides only `Redis`. A command goes to the
   * node that serves the slot of its first key. `MOVED` and `ASK` replies are followed. A command whose keys are in
   * different slots fails with `CrossSlot` after the retries of `RetryClusterConfig`.
   */
  lazy val cluster: ZLayer[CodecSupplier & RedisClusterConfig, RedisError, Redis] =
    ZLayer.makeSome[CodecSupplier & RedisClusterConfig, Redis](ClusterExecutor.layer, makeLayer)

  /**
   * A client for the Redis server at `localhost` on port 6379. It provides `Redis` and `AsyncRedis`. The connection
   * opens when the layer is built, and the layer fails with `RedisError.IOError` when the connection cannot be opened.
   */
  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, Redis & AsyncRedis] =
    SingleNodeExecutor.local >>> makeLayer

  /**
   * A client for the Redis server in the `RedisConfig`. It provides `Redis` and `AsyncRedis`. Like `local`, it opens
   * the connection when the layer is built. It sends the `AUTH` command when the config has credentials.
   */
  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, Redis & AsyncRedis] =
    SingleNodeExecutor.layer >>> makeLayer
  private def makeLayer: URLayer[CodecSupplier & RedisExecutor, AsyncRedis & Redis]                =
    ZLayer.fromZIOEnvironment {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[RedisExecutor]
      } yield ZEnvironment[AsyncRedis, Redis](
        new AsyncLive(codecSupplier, executor),
        new SyncLive(codecSupplier, executor)
      )
    }

  private final class SyncLive(val codecSupplier: CodecSupplier, val executor: RedisExecutor) extends Redis {
    protected def lift[A](in: UIO[IO[RedisError, A]]): GenRedis.Sync[A] = GenRedis.sync(in)
  }

  private final class AsyncLive(val codecSupplier: CodecSupplier, val executor: RedisExecutor) extends AsyncRedis {
    protected def lift[A](in: UIO[IO[RedisError, A]]): GenRedis.Async[A] = GenRedis.async(in)
  }
}
