package zio.redis

// import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A] {
  private[redis] def encode(data: A): Chunk[String]
}

object Input {
  // private[this] def wrap(text: String): String = s"$$${text.length}\r\n$text\r\n"

  // def chunk(args: Chunk[String]): Chunk[Byte] = {
  //   val wrapped  = args.map(wrap).mkString
  //   val envelope = s"*${args.length}\r\n$wrapped"
  //   Chunk.fromArray(envelope.getBytes(UTF_8))
  // }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): Chunk[String] = ???
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): Chunk[String] = ???
  }

  case object BitFieldGetInput extends Input[BitFieldGet] {
    def encode(data: BitFieldGet): Chunk[String] = ???
  }

  case object BitFieldSetInput extends Input[BitFieldSet] {
    def encode(data: BitFieldSet): Chunk[String] = ???
  }

  case object BitFieldIncrInput extends Input[BitFieldIncr] {
    def encode(data: BitFieldIncr): Chunk[String] = ???
  }

  case object BitFieldOverflowInput extends Input[BitFieldOverflow] {
    def encode(data: BitFieldOverflow): Chunk[String] = ???
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): Chunk[String] = ???
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): Chunk[String] = ???
  }

  case object ByteInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): Chunk[String] = ???
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): Chunk[String] = ???
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): Chunk[String] = ???
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): Chunk[String] = ???
  }

  case object DurationInput extends Input[Duration] {
    def encode(data: Duration): Chunk[String] = ???
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): Chunk[String] = ???
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): Chunk[String] = ???
  }

  case object LexRangeInput extends Input[LexRange] {
    def encode(data: LexRange): Chunk[String] = ???
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): Chunk[String] = ???
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): Chunk[String] = ???
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): Chunk[String] = ???
  }

  case object MemberScoreInput extends Input[MemberScore] {
    def encode(data: MemberScore): Chunk[String] = ???
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): Chunk[String] = ???
  }

  final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): Chunk[String] = ???
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): Chunk[String] = ???
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): Chunk[String] = ???
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): Chunk[String] = ???
  }

  case object RegexInput extends Input[Regex] {
    def encode(data: Regex): Chunk[String] = ???
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): Chunk[String] = ???
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): Chunk[String] = ???
  }

  case object ScoreRangeInput extends Input[ScoreRange] {
    def encode(data: ScoreRange): Chunk[String] = ???
  }

  case object StringInput extends Input[String] {
    def encode(data: String): Chunk[String] = ???
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): Chunk[String] = ???
  }

  case object TimeInput extends Input[Instant] {
    def encode(data: Instant): Chunk[String] = ???
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): Chunk[String] = ???
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): Chunk[String] = ???
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): Chunk[String] = ???
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): Chunk[String] = ???
  }

  final case class Tuple7[-A, -B, -C, -D, -E, -F, -G](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G]
  ) extends Input[(A, B, C, D, E, F, G)]

  final case class Tuple9[-A, -B, -C, -D, -E, -F, -G, -H, -I](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I]
  ) extends Input[(A, B, C, D, E, F, G, H, I)]

  final case class Tuple11[-A, -B, -C, -D, -E, -F, -G, -H, -I, -J, -K](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I],
    _10: Input[J],
    _11: Input[K]
  ) extends Input[(A, B, C, D, E, F, G, H, I, J, K)] {
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): Chunk[String] = ???
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): Chunk[String] = ???
  }

  final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): Chunk[String] = ???
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): Chunk[String] = ???
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): Chunk[String] = ???
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): Chunk[String] = ???
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): Chunk[String] = ???
  }
}
