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

import zio._
import zio.redis._
import zio.schema._
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

private[benchmarks] final class ZioRedis private (runtime: Runtime.Scoped[Redis]) {
  def run(program: ZIO[Redis, RedisError, Unit]): Unit =
    Unsafe.unsafe(implicit u => runtime.unsafe.run(program).getOrThrow())

  def tearDown(): Unit =
    Unsafe.unsafe(implicit u => runtime.unsafe.shutdown())
}

private[benchmarks] object ZioRedis {
  def unsafeMake(): ZioRedis = {
    val runtime = Unsafe.unsafe(implicit u => Runtime.unsafe.fromLayer(TestLayer))
    new ZioRedis(runtime)
  }

  private object ProtobufCodecSupplier extends CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
  }

  private final val TestLayer =
    ZLayer.make[Redis](Redis.local, ZLayer.succeed(ProtobufCodecSupplier))
}
