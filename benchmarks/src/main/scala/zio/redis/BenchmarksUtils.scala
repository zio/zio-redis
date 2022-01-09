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

import cats.effect.{IO => CatsIO}

import zio.logging.Logging
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec
import zio.{BootstrapRuntime, ZIO, ZLayer}

trait BenchmarksUtils {
  self: RedisClients with BootstrapRuntime =>

  def unsafeRun[CL](f: CL => CatsIO[Unit])(implicit unsafeRunner: QueryUnsafeRunner[CL]): Unit =
    unsafeRunner.unsafeRun(f)

  def zioUnsafeRun(source: ZIO[RedisExecutor, RedisError, Unit]): Unit =
    unsafeRun(source.provideLayer(BenchmarksUtils.Layer))
}

object BenchmarksUtils {
  private final val Layer = Logging.ignore ++ ZLayer.succeed[Codec](StringUtf8Codec) >>> RedisExecutor.local.orDie
}
