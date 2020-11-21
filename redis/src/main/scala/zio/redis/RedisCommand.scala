package zio.redis

import zio.ZIO
import zio.stream.ZStream

final class RedisCommand[-In, +Out] private (name: String, input: Input[In], output: Output[Out]) {
  private[redis] def run(in: In): ZIO[RedisExecutor, RedisError, Out] =
    ZIO
      .accessM[RedisExecutor](_.get.execute(Input.StringInput.encode(name) ++ input.encode(in)))
      .flatMap(out => ZIO.effect(output.unsafeDecode(out)))
      .refineToOrDie[RedisError]

  private[redis] def runStream(in: In): ZStream[RedisExecutor, RedisError, Out] =
    for {
      re  <- ZStream.fromEffect(ZIO.access[RedisExecutor](_.get))
      out <- re.executeStream(Input.StringInput.encode(name) ++ input.encode(in))
      rv  <- ZStream.fromEffect(ZIO.effect(output.unsafeDecode(out)).refineToOrDie[RedisError])
    } yield rv
}

object RedisCommand {
  private[redis] def apply[In, Out](name: String, input: Input[In], output: Output[Out]): RedisCommand[In, Out] =
    new RedisCommand(name, input, output)
}
