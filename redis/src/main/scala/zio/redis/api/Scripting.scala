package zio.redis.api

import zio.redis.Input.EvalInput
import zio.redis.Output.RespValueOutput
import zio.redis._
import zio.{ Chunk, ZIO }

trait Scripting {
  import Scripting._

  def eval[K: RedisEncoder, A: RedisEncoder, R: RedisDecoder](script: Script[K, A]): ZIO[RedisExecutor, RedisError, R] =
    Eval
      .run((script.lua, script.encodeKeys, script.encodeArgs))
      .flatMap(v => implicitly[RedisDecoder[R]].decode(v))
}

private[redis] object Scripting {

  val Eval: RedisCommand[(String, Seq[Chunk[Byte]], Seq[Chunk[Byte]]), RespValue] =
    RedisCommand("EVAL", EvalInput, RespValueOutput)
}
