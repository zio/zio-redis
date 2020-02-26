package zio.redis

import java.time.Instant

import zio.{ Chunk, IO }
import zio.stream.Stream
import zio.duration.Duration

import scala.util.matching.Regex

final case class Command[-In, +Out] private[redis] (
  name: String,
  input: Command.Input[In],
  output: Command.Output[Out]
) {

  // main command interpreter
  def run(in: In): Out = ???
}

object Command {
  implicit final class Arg0[+Out](private val command: Command[Unit, Out]) extends AnyVal {
    def apply(): Out = command.run(())
  }

  implicit final class Arg1[-A, +Out](private val command: Command[A, Out]) extends AnyVal {
    def apply(a: A): Out = command.run(a)
  }

  implicit final class Arg1Varargs[-A, -B, +Out](private val command: Command[(A, (B, List[B])), Out]) extends AnyVal {
    def apply(a: A)(b: B, bs: B*): Out = command.run((a, (b, bs.toList)))
  }

  implicit final class Arg2[-A, -B, +Out](private val command: Command[(A, B), Out]) extends AnyVal {
    def apply(a: A, b: B): Out = command.run((a, b))
  }

  implicit final class Arg3[-A, -B, -C, +Out](private val command: Command[(A, B, C), Out]) extends AnyVal {
    def apply(a: A, b: B, c: C): Out = command.run((a, b, c))
  }

  sealed trait Input[-A]

  object Input {
    case object ByteInput     extends Input[Chunk[Byte]]
    case object CountInput    extends Input[Long]
    case object DoubleInput   extends Input[Double]
    case object DurationInput extends Input[Duration]
    case object LongInput     extends Input[Long]
    case object MatchInput    extends Input[Regex]
    case object UnitInput     extends Input[Unit]
    case object RangeInput    extends Input[Range]
    case object StringInput   extends Input[String]
    case object TimeInput     extends Input[Instant]
    case object TypeInput     extends Input[String]

    final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]]

    final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])]

    final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)]

    final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]

    final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
        extends Input[(A, B, C, D)]

    final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]
  }

  sealed trait Output[+A]

  object Output {
    case object BoolOutput     extends Output[IO[Error, Boolean]]
    case object ByteOutput     extends Output[IO[Error, Chunk[Byte]]]
    case object DurationOutput extends Output[IO[Error, Duration]]
    case object LongOutput     extends Output[IO[Error, Long]]
    case object ScanOutput     extends Output[IO[Error, (Long, Stream[Error, Chunk[Byte]])]]
    case object StreamOutput   extends Output[Stream[Error, Chunk[Byte]]]
    case object StringOutput   extends Output[IO[Error, String]]
    case object UnitOutput     extends Output[IO[Error, Unit]]
  }
}
