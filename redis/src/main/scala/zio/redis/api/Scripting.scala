package zio.redis.api

import zio.redis.Input.EvalInput
import zio.redis.Output.RespValueOutput
import zio.redis._
import zio.{ Chunk, ZIO }

trait Scripting {
  import Scripting._

  def eval[K: Encoder, A: Encoder, R: Decoder](
    script: String,
    keys: Chunk[K],
    args: Chunk[A]
  ): ZIO[RedisExecutor, RedisError, R] = {
    val encodeKey  = implicitly[Encoder[K]].encode _
    val encodeArg  = implicitly[Encoder[A]].encode _
    val decodeResp = implicitly[Decoder[R]].decode _
    Eval.run((script, keys.map(encodeKey), args.map(encodeArg))).flatMap(decodeResp)
  }
}

private[redis] object Scripting {

  val Eval: RedisCommand[(String, Chunk[Chunk[Byte]], Chunk[Chunk[Byte]]), RespValue] =
    RedisCommand("EVAL", EvalInput, RespValueOutput)
}
