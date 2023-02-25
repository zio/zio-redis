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
import zio.redis.Input.{CommandNameInput, Varargs}
import zio.schema.codec.BinaryCodec

final class RedisCommand[-In, +Out] private (
  val name: String,
  val input: Input[In],
  val output: Output[Out],
  val codec: BinaryCodec,
  val executor: RedisExecutor
) {

  private[redis] def run(in: In): IO[RedisError, Out] =
    executor
      .execute(resp(in))
      .flatMap[Any, Throwable, Out](out => ZIO.attempt(output.unsafeDecode(out)(codec)))
      .refineToOrDie[RedisError]

  private[redis] def resp(in: In): RespCommand =
    Varargs(CommandNameInput).encode(name.split(" "))(codec) ++ input.encode(in)(codec)
}

object RedisCommand {
  private[redis] def apply[In, Out](
    name: String,
    input: Input[In],
    output: Output[Out],
    codec: BinaryCodec,
    executor: RedisExecutor
  ): RedisCommand[In, Out] =
    new RedisCommand(name, input, output, codec, executor)
}
