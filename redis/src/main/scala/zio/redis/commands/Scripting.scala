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

import zio.Chunk
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

private[redis] trait Scripting extends RedisEnvironment {
  import Scripting._

  final def _eval[K: Input, A: Input, R: Output]: RedisCommand[(String, Chunk[K], Chunk[A]), R] =
    RedisCommand(Eval, EvalInput(Input[K], Input[A]), Output[R], codec, executor)

  final def _evalSha[K: Input, A: Input, R: Output]: RedisCommand[(String, Chunk[K], Chunk[A]), R] =
    RedisCommand(EvalSha, EvalInput(Input[K], Input[A]), Output[R], codec, executor)

  final val _scriptDebug: RedisCommand[DebugMode, Unit] =
    RedisCommand(Scripting.ScriptDebug, ScriptDebugInput, UnitOutput, codec, executor)

  final val _scriptExists: RedisCommand[(String, List[String]), Chunk[Boolean]] =
    RedisCommand(Scripting.ScriptExists, NonEmptyList(StringInput), ChunkOutput(BoolOutput), codec, executor)

  final val _scriptFlush: RedisCommand[Option[FlushMode], Unit] =
    RedisCommand(Scripting.ScriptFlush, OptionalInput(ScriptFlushInput), UnitOutput, codec, executor)

  final val _scriptKill: RedisCommand[Unit, Unit] =
    RedisCommand(Scripting.ScriptKill, NoInput, UnitOutput, codec, executor)

  final val _scriptLoad: RedisCommand[String, String] =
    RedisCommand(Scripting.ScriptLoad, StringInput, MultiStringOutput, codec, executor)
}

private object Scripting {
  final val Eval         = "EVAL"
  final val EvalSha      = "EVALSHA"
  final val ScriptDebug  = "SCRIPT DEBUG"
  final val ScriptExists = "SCRIPT EXISTS"
  final val ScriptFlush  = "SCRIPT FLUSH"
  final val ScriptKill   = "SCRIPT KILL"
  final val ScriptLoad   = "SCRIPT LOAD"
}
