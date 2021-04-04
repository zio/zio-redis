package zio.redis

import zio.ZIO
import zio.redis.Input.{ StringInput, Varargs }

final class RedisCommand[-In, +Out] private (val name: String, val input: Input[In], val output: Output[Out]) {
  private[redis] def run(in: In): ZIO[RedisExecutor, RedisError, Out] =
    ZIO
      .accessM[RedisExecutor] { executor =>
        val service = executor.get
        val codec   = service.codec
        val command = Varargs(StringInput).encode(name.split(" "))(codec) ++ input.encode(in)(codec)
        service.execute(command).flatMap[Any, Throwable, Out](out => ZIO.effect(output.unsafeDecode(out)(codec)))
      }
      .refineToOrDie[RedisError]
}

object RedisCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}
