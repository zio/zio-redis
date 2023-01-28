package zio.redis

import zio._
import zio.redis.Input.{StringInput, Varargs}
import zio.schema.codec.BinaryCodec

class Executable[-In, +Out](cmd: RedisCommand[In, Out], data: In) {
  def execute: ZIO[RedisExecutor & BinaryCodec, RedisError, Out] = for {
    executor <- ZIO.service[RedisExecutor]
    codec    <- ZIO.service[BinaryCodec]
    out <- executor
             .execute(resp(data, codec))
             .flatMap[Any, Throwable, Out](out => ZIO.attempt(cmd.output.unsafeDecode(out)(codec)))
             .refineToOrDie[RedisError]
  } yield out
  private[redis] def resp(in: In, codec: BinaryCodec): Chunk[RespValue.BulkString] =
    Varargs(StringInput).encode(cmd.name.split(" "))(codec) ++ cmd.input.encode(in)(codec)
}

object Executable {
  def apply[In, Out](cmd: RedisCommand[In, Out], data: In): Executable[In, Out] = new Executable(cmd, data)
}
