package zio.redis

import zio.{ Chunk, IO }
import zio.stream.Stream

final case class Command[-In, +Out] private[redis] (
  name: String,
  input: Command.Input[In],
  output: Command.Output[Out]
) {
  def apply()(implicit ev: Any <:< In): Out = run(ev)

  def apply[A](a: A)(implicit ev: A <:< In): Out = run(a)

  def apply[A, B](a: A, b: B)(implicit ev: (A, B) <:< In): Out = run((a, b))

  def apply[A, B, C](a: A, b: B, c: C)(implicit ev: (A, B, C) <:< In): Out = run((a, b, c))

  // main command interpreter
  private def run(in: In): Out = ???
}

object Command {
  sealed trait Input[-A]

  object Input {
    case object KeyInput                                                          extends Input[String]
    case object StringInput                                                       extends Input[String]
    case object NoInput                                                           extends Input[Any]
    case object RangeInput                                                        extends Input[Range]
    case object LongInput                                                         extends Input[Long]
    case object DoubleInput                                                       extends Input[Double]
    final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B])                   extends Input[(A, B)]
    final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]
    final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
        extends Input[(A, B, C, D)]
    final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]
  }

  sealed trait Output[+A]

  object Output {
    case object UnitOutput   extends Output[IO[Error, Unit]]
    case object LongOutput   extends Output[IO[Error, Long]]
    case object BoolOutput   extends Output[IO[Error, Boolean]]
    case object ValueOutput  extends Output[IO[Error, Chunk[Byte]]]
    case object StreamOutput extends Output[Stream[Error, Chunk[Byte]]]
  }
}
