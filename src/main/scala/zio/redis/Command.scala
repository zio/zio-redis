package zio.redis

import zio.{ Chunk, IO }
import zio.stream.Stream

final case class Command[-In, +Out] private[redis] (
  name: String,
  private[redis] input: Command.Input[In],
  private[redis] output: Command.Output[Out]
) {
  // TODO: main command interpreter
  def apply(input: In): Out = ???

  def apply[A, B, In1 <: In](a: A, b: B)(implicit ev: (A, B) <:< In1): Out = apply((a, b))

  def apply[A, B, C, In1 <: In](a: A, b: B, c: C)(implicit ev: (A, B, C) <:< In1): Out = apply((a, b, c))
}

object Command {
  sealed trait Input[-A]

  object Input {
    final case object KeyInput                                                    extends Input[String]
    final case object StringInput                                                 extends Input[String]
    final case object RangeInput                                                  extends Input[Range]
    final case object LongInput                                                   extends Input[Long]
    final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B])                   extends Input[(A, B)]
    final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]
    final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
        extends Input[(A, B, C, D)]
    final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]
  }

  sealed trait Output[+A]

  object Output {
    final case object UnitOutput   extends Output[IO[Error, Unit]]
    final case object LongOutput   extends Output[IO[Error, Long]]
    final case object ValueOutput  extends Output[IO[Error, Chunk[Byte]]]
    final case object StreamOutput extends Output[Stream[Error, Chunk[Byte]]]
  }
}
