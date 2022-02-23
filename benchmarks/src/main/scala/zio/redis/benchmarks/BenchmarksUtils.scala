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

package zio.redis.benchmarks

import cats.effect.{IO => CIO}
import zio.{BootstrapRuntime, Has, ZIO, ZLayer}
import zio.logging.Logging
import zio.redis._
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec

trait BenchmarksUtils {
  self: BootstrapRuntime =>

  final def unsafeRun[Client: QueryRunner](query: Client => CIO[Unit]): Unit =
    QueryRunner[Client].unsafeRunWith(query)

  final def zioUnsafeRun(query: ZIO[Has[Redis], RedisError, Unit]): Unit =
    unsafeRun(query.provideLayer(BenchmarksUtils.Layer))
}

object BenchmarksUtils {
  private final val Layer = {
    val executor = Logging.ignore >>> RedisExecutor.local.orDie
    executor ++ ZLayer.succeed[Codec](StringUtf8Codec) >>> Redis.live
  }
}
