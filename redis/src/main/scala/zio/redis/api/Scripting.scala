package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Scripting {
  import Scripting._

  /**
   * Evaluates a Lua script
   *
   * @param script Lua script
   * @param keys keys available through KEYS param in the script
   * @param args values available through ARGV param in the script
   * @return redis protocol value that is converted from the Lua type.
   *         You have to write decoder that would convert
   *         redis protocol value to a suitable type for your app
   */
  def eval[K: Input, A: Input, R: Output](
    script: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ZIO[RedisExecutor, RedisError, R] =
    Eval[K, A, R].run((script, keys, args))

  /**
   * Evaluates a Lua script cached on the server side by its SHA1 digest.
   * Scripts could be cached using the [[zio.redis.api.Scripting.scriptLoad]] method.
   *
   * @param sha1 SHA1 digest
   * @param keys keys available through KEYS param in the script
   * @param args values available through ARGV param in the script
   * @return redis protocol value that is converted from the Lua type.
   *         You have to write decoder that would convert
   *         redis protocol value to a suitable type for your app
   */
  def evalSha[K: Input, A: Input, R: Output](
    sha1: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ZIO[RedisExecutor, RedisError, R] =
    EvalSha[K, A, R].run((sha1, keys, args))

  /**
   * Checks existence of the scripts in the script cache.
   *
   * @param sha1 one required SHA1 digest
   * @param sha1s maybe rest of the SHA1 digests
   * @return for every corresponding SHA1 digest of a script that actually exists in the script cache,
   *         an true is returned, otherwise false is returned.
   */
  def scriptExists(sha1: String, sha1s: String*): ZIO[RedisExecutor, RedisError, Chunk[Boolean]] =
    ScriptExists.run((sha1, sha1s.toList))

  /**
   * Loads a script into the scripts cache.
   * After the script is loaded into the script cache it could be evaluated using the [[zio.redis.api.Scripting.evalSha]] method.
   *
   * @param script Lua script
   * @return the SHA1 digest of the script added into the script cache.
   */
  def scriptLoad(script: String): ZIO[RedisExecutor, RedisError, String] =
    ScriptLoad.run(script)
}

private[redis] object Scripting {

  final def Eval[K: Input, A: Input, R: Output]: RedisCommand[(String, Chunk[K], Chunk[A]), R] =
    RedisCommand("EVAL", EvalInput(implicitly[Input[K]], implicitly[Input[A]]), implicitly[Output[R]])

  final def EvalSha[K: Input, A: Input, R: Output]: RedisCommand[(String, Chunk[K], Chunk[A]), R] =
    RedisCommand("EVALSHA", EvalInput(implicitly[Input[K]], implicitly[Input[A]]), implicitly[Output[R]])

  final val ScriptExists: RedisCommand[(String, List[String]), Chunk[Boolean]] =
    RedisCommand("SCRIPT EXISTS", NonEmptyList(StringInput), ChunkOutput(BoolOutput))

  final val ScriptLoad: RedisCommand[String, String] =
    RedisCommand("SCRIPT LOAD", StringInput, MultiStringOutput)

}
