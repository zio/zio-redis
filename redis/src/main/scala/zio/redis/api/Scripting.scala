package zio.redis.api

import zio.{Chunk, ZIO}
import zio.redis._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultOutputBuilder

trait Scripting {
  import Scripting._

  /**
   * Evaluates a Lua script
   *
   * Example of custom data:
   * {{{
   * final case class Person(name: String, age: Long)
   *
   * val encoder = Input.Tuple2(StringInput, LongInput).contramap[Person] { civ =>
   *   (civ.name, civ.age)
   * }
   * val decoder = RespValueOutput.map {
   *   case RespValue.Array(elements) =>
   *     val name = elements(0) match {
   *       case s @ RespValue.BulkString(_) => s.asString
   *       case other                       => throw ProtocolError(s"$other isn't a string type")
   *     }
   *     val age = elements(1) match {
   *       case RespValue.Integer(value) => value
   *       case other                    => throw ProtocolError(s"$other isn't a integer type")
   *     }
   *     Person(name, age)
   *   case other => throw ProtocolError(s"$other isn't an array type")
   * }
   * }}}
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
  def eval[K: Input, A: Input](
    script: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: ZIO[RedisExecutor, RedisError, R] = {
      val command = RedisCommand(Eval, EvalInput(implicitly[Input[K]], implicitly[Input[A]]), implicitly[Output[R]])
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
  def evalSha[K: Input, A: Input](
    sha1: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ResultOutputBuilder = new ResultOutputBuilder {
    def returning[R: Output]: ZIO[RedisExecutor, RedisError, R] = {
      val command = RedisCommand(EvalSha, EvalInput(implicitly[Input[K]], implicitly[Input[A]]), implicitly[Output[R]])
      command.run((sha1, keys, args))
    }
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
  def scriptExists(sha1: String, sha1s: String*): ZIO[RedisExecutor, RedisError, Chunk[Boolean]] = {
    val command = RedisCommand(ScriptExists, NonEmptyList(StringInput), ChunkOutput(BoolOutput))
    command.run((sha1, sha1s.toList))
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
  def scriptLoad(script: String): ZIO[RedisExecutor, RedisError, String] = {
    val command = RedisCommand(ScriptLoad, StringInput, MultiStringOutput)
    command.run(script)
  }
}

private[redis] object Scripting {
  final val Eval         = "EVAL"
  final val EvalSha      = "EVALSHA"
  final val ScriptExists = "SCRIPT EXISTS"
  final val ScriptLoad   = "SCRIPT LOAD"
}
