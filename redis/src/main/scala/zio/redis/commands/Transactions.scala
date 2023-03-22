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

package zio.redis.commands

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

private[redis] trait Transactions extends RedisEnvironment {
  import Transactions._

  final val _multi: RedisCommand[Unit, Unit] = RedisCommand(Multi, NoInput, UnitOutput, codec, executor)

  final def _exec[Out](output: Output[Out]): RedisCommand[Unit, Out] =
    RedisCommand(Exec, NoInput, output, codec, executor)
}

private object Transactions {
  final val Multi = "Multi"
  final val Exec  = "Exec"
}
