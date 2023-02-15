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

package zio.redis.api

import zio._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultOutputBuilder
import zio.redis._

trait Scripting extends RedisEnvironment {
  import Scripting._

  /**
   * Evaluates a Lua script.
   *
   * @param script
   *   Lua script
   * @param keys
   *   keys available through KEYS param in the script
   * @param args
   *   values available through ARGV param in the script
   * @return
   *   redis protocol value that is converted from the Lua type. You have to write decoder that would convert redis
   *   protocol value to a suitable type for your app
   */
  final def eval[K: Input, A: Input](
    script: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: IO[RedisError, R] = {
      val command = RedisCommand(Eval, EvalInput(Input[K], Input[A]), Output[R], codec, executor)
      command.run((script, keys, args))
    }
  }

  /**
   * Evaluates a Lua script cached on the server side by its SHA1 digest. Scripts could be cached using the
   * [[zio.redis.api.Scripting.scriptLoad]] method.
   *
   * @param sha1
   *   SHA1 digest
   * @param keys
   *   keys available through KEYS param in the script
   * @param args
   *   values available through ARGV param in the script
   * @return
   *   redis protocol value that is converted from the Lua type. You have to write decoder that would convert redis
   *   protocol value to a suitable type for your app
   */
  final def evalSha[K: Input, A: Input](
    sha1: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: IO[RedisError, R] = {
      val command = RedisCommand(EvalSha, EvalInput(Input[K], Input[A]), Output[R], codec, executor)
      command.run((sha1, keys, args))
    }
  }

  /**
   * Set the debug mode for subsequent scripts executed with [[zio.redis.api.Scripting.eval]].
   *
   * @param mode
   *   - [[zio.redis.DebugMode.Yes]]: Enable non-blocking asynchronous debugging of Lua scripts (changes are discarded).
   *   - [[zio.redis.DebugMode.Sync]]: Enable blocking synchronous debugging of Lua scripts (saves changes to data).
   *   - [[zio.redis.DebugMode.No]]: Disables scripts debug mode.
   * @return
   *   the Unit value.
   */
  final def scriptDebug(mode: DebugMode): IO[RedisError, Unit] = {
    val command = RedisCommand(ScriptDebug, ScriptDebugInput, UnitOutput, codec, executor)
    command.run(mode)
  }

  /**
   * Checks existence of the scripts in the script cache.
   *
   * @param sha1
   *   one required SHA1 digest
   * @param sha1s
   *   maybe rest of the SHA1 digests
   * @return
   *   for every corresponding SHA1 digest of a script that actually exists in the script cache, an true is returned,
   *   otherwise false is returned.
   */
  final def scriptExists(sha1: String, sha1s: String*): IO[RedisError, Chunk[Boolean]] = {
    val command = RedisCommand(ScriptExists, NonEmptyList(StringInput), ChunkOutput(BoolOutput), codec, executor)
    command.run((sha1, sha1s.toList))
  }

  /**
   * Remove all the scripts from the script cache.
   *
   * @param mode
   *   mode in which script flush is going to be executed ["ASYNC", "SYNC"] Note: "SYNC" mode is used by default (if no
   *   mode is provided)
   *   - [[zio.redis.FlushMode.Async]]: Flushes the cache asynchronously.
   *   - [[zio.redis.FlushMode.Sync]]: Flushes the cache synchronously.
   *
   * Note: If no mode is provided, command is going to be executed in [[zio.redis.FlushMode.Sync]] mode.
   * @return
   *   the Unit value.
   */
  final def scriptFlush(mode: Option[FlushMode] = None): IO[RedisError, Unit] = {
    val command = RedisCommand(ScriptFlush, OptionalInput(ScriptFlushInput), UnitOutput, codec, executor)
    command.run(mode)
  }

  /**
   * Kill the currently executing [[zio.redis.api.Scripting.eval]] script, assuming no write operation was yet performed
   * by the script.
   *
   * @return
   *   the Unit value.
   */
  final def scriptKill: IO[RedisError, Unit] = {
    val command = RedisCommand(ScriptKill, NoInput, UnitOutput, codec, executor)
    command.run(())
  }

  /**
   * Loads a script into the scripts cache. After the script is loaded into the script cache it could be evaluated using
   * the [[zio.redis.api.Scripting.evalSha]] method.
   *
   * @param script
   *   Lua script
   * @return
   *   the SHA1 digest of the script added into the script cache.
   */
  final def scriptLoad(script: String): IO[RedisError, String] = {
    val command = RedisCommand(ScriptLoad, StringInput, MultiStringOutput, codec, executor)
    command.run(script)
  }
}

private[redis] object Scripting {
  final val Eval         = "EVAL"
  final val EvalSha      = "EVALSHA"
  final val ScriptDebug  = "SCRIPT DEBUG"
  final val ScriptExists = "SCRIPT EXISTS"
  final val ScriptFlush  = "SCRIPT FLUSH"
  final val ScriptKill   = "SCRIPT KILL"
  final val ScriptLoad   = "SCRIPT LOAD"
}
