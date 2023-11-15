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

package zio.redis.internal

import zio.redis.{CodecSupplier, RedisError}
import zio.schema.Schema
import zio.schema.codec.BinaryCodec
import zio.{IO, UIO, ZIO}

private[redis] trait RedisEnvironment[G[+_]] {
  protected def codecSupplier: CodecSupplier
  protected def executor: RedisExecutor
  implicit class RunOps[In, Out](cmd: RedisCommand[In, Out]) {
    def run(in: In): G[Out] = toG(
      executor
        .execute(cmd.resp(in))
        .map(_.flatMap(out => ZIO.attempt(cmd.output.unsafeDecode(out))).refineToOrDie[RedisError])
    )
  }

  def toG[A](in: UIO[IO[RedisError, A]]): G[A]

  protected final implicit def codec[A: Schema]: BinaryCodec[A] = codecSupplier.get
}
