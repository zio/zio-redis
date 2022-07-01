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
import zio.redis._
import zio.schema.codec.{Codec, ProtobufCodec}
import zio.{ZIO, ZLayer}
import zio.Unsafe

trait BenchmarkRuntime {

  final def execute(query: ZIO[Redis, RedisError, Unit]): Unit = {
    implicit val un: Unsafe = zio.Unsafe.unsafe
    zio.Runtime.default.unsafe.run(query.provideLayer(BenchmarkRuntime.Layer)).getOrThrowFiberFailure()
  }

  final def execute[Client: QueryRunner](query: Client => CIO[Unit]): Unit =
    QueryRunner[Client].unsafeRunWith(query)
}

object BenchmarkRuntime {
  private final val Layer = {
    val executor = RedisExecutor.local.orDie
    executor ++ ZLayer.succeed[Codec](ProtobufCodec) >>> Redis.live
  }
}
