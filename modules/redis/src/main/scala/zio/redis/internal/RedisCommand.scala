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

import zio.redis.Input.{CommandNameInput, Varargs}
import zio.redis._

private[redis] final class RedisCommand[-In, +Out] private (
  val name: String,
  val input: Input[In],
  val output: Output[Out]
) {
  def resp(in: In): RespCommand =
    Varargs(CommandNameInput).encode(name.split(" ")) ++ input.encode(in)
}

private[redis] object RedisCommand {
  def apply[In, Out, G[+_]](
    name: String,
    input: Input[In],
    output: Output[Out]
  ): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}
