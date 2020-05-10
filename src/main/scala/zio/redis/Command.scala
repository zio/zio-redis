package zio.redis

import zio.IO

final case class Command[-In, +Out] private[redis] (
  name: String,
  input: Input[In],
  output: Output[Out]
) {

  // main command interpreter
  def run(in: In): IO[RedisError, Out] = ???
}

object Command {
  implicit final class Arg0[+Out](private val command: Command[Unit, Out]) extends AnyVal {
    def apply(): IO[RedisError, Out] = command.run(())
  }

  implicit final class Arg1[-A, +Out](private val command: Command[A, Out]) extends AnyVal {
    def apply(a: A): IO[RedisError, Out] = command.run(a)
  }

  implicit final class Arg2[-A, -B, +Out](private val command: Command[(A, B), Out]) extends AnyVal {
    def apply(a: A, b: B): IO[RedisError, Out] = command.run((a, b))
  }

  implicit final class Arg3[-A, -B, -C, +Out](private val command: Command[(A, B, C), Out]) extends AnyVal {
    def apply(a: A, b: B, c: C): IO[RedisError, Out] = command.run((a, b, c))
  }

  implicit final class Arg4[-A, -B, -C, -D, +Out](private val command: Command[(A, B, C, D), Out]) extends AnyVal {
    def apply(a: A, b: B, c: C, d: D): IO[RedisError, Out] = command.run((a, b, c, d))
  }

  implicit final class Arg5[-A, -B, -C, -D, -E, +Out](private val command: Command[(A, B, C, D, E), Out])
      extends AnyVal {
    def apply(a: A, b: B, c: C, d: D, e: E): IO[RedisError, Out] = command.run((a, b, c, d, e))
  }

  implicit final class Arg11[-A, -B, -C, -D, -E, -F, -G, -H, -I, -J, -K, +Out](
    private val command: Command[(A, B, C, D, E, F, G, H, I, J, K), Out]
  ) extends AnyVal {
    def apply(a: A, b: B, c: C, d: D, e: E, f: F, g: G, h: H, i: I, j: J, k: K): IO[RedisError, Out] =
      command.run((a, b, c, d, e, f, g, h, i, j, k))
  }

  implicit final class Arg1Varargs[-A, -B, +Out](private val command: Command[(A, (B, List[B])), Out]) extends AnyVal {
    def apply(a: A)(b: B, bs: B*): IO[RedisError, Out] = command.run((a, (b, bs.toList)))
  }

  implicit final class Arg1VarargsArg1[-A, -B, -C, +Out](private val command: Command[(A, (B, List[B]), C), Out])
      extends AnyVal {
    def apply(a: A)(b: B, bs: B*)(c: C): IO[RedisError, Out] = command.run((a, (b, bs.toList), c))
  }

  implicit final class Arg2VarargsArg2[-A, -B, -C, -D, -E, +Out](
    private val command: Command[(A, B, (C, List[C]), D, E), Out]
  ) extends AnyVal {
    def apply(a: A, b: B)(c: C, cs: C*)(d: D, e: E): IO[RedisError, Out] = command.run((a, b, (c, cs.toList), d, e))
  }
}
