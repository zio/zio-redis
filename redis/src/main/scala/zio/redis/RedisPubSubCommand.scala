package zio.redis

import zio.redis.Input.StringInput
import zio.redis.Output.PushProtocolOutput
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, ZIO}

case class RedisPubSubCommand[-In, +Out](name: String, input: Input[In], output: Output[Out]) {
  private[redis] def run(in: In): ZStream[Redis, RedisError, PushProtocol[Out]] =
    ZStream.serviceWithStream[Redis] { redis =>
      redis.executor
        .executePubSub(resp(in)(redis.codec))
        .mapZIO(out => ZIO.attempt(PushProtocolOutput(output).unsafeDecode(out)(redis.codec)))
        .refineToOrDie[RedisError]
    }

  private def resp(in: In)(implicit codec: BinaryCodec): Chunk[RespValue.BulkString] =
    StringInput.encode(name) ++ input.encode(in)
}
