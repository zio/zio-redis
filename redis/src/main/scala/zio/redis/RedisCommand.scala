package zio.redis

import zio.{ Chunk, ZIO }

final class RedisCommand[-In, +Out] private (name: Chunk[String], input: Input[In], output: Output[Out]) {
  private[redis] def run(in: In): ZIO[RedisExecutor, RedisError, Out] =
    ZIO
      .accessM[RedisExecutor](_.get.execute(name.map(Input.stringEncode) ++ input.encode(in)))
      .flatMap(out => ZIO.effect(output.unsafeDecode(out)))
      .refineToOrDie[RedisError]
}

object RedisCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(Chunk.single(name), input, output)

  private[redis] def apply[In, Out](name: Chunk[String], input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}
