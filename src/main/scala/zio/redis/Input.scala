package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A] {
  private[redis] def encode(data: A): Chunk[Byte]
}

object Input {
  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): Chunk[Byte] = ???
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): Chunk[Byte] = ???
  }

  case object BitFieldGetInput extends Input[BitFieldGet] {
    def encode(data: BitFieldGet): Chunk[Byte] = ???
  }

  case object BitFieldSetInput extends Input[BitFieldSet] {
    def encode(data: BitFieldSet): Chunk[Byte] = ???
  }

  case object BitFieldIncrInput extends Input[BitFieldIncr] {
    def encode(data: BitFieldIncr): Chunk[Byte] = ???
  }

  case object BitFieldOverflowInput extends Input[BitFieldOverflow] {
    def encode(data: BitFieldOverflow): Chunk[Byte] = ???
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): Chunk[Byte] = ???
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): Chunk[Byte] = ???
  }

  case object ByteInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): Chunk[Byte] = ???
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): Chunk[Byte] = ???
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): Chunk[Byte] = ???
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): Chunk[Byte] = ???
  }

  case object DurationInput extends Input[Duration] {
    def encode(data: Duration): Chunk[Byte] = ???
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): Chunk[Byte] = ???
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): Chunk[Byte] = ???
  }

  case object LexRangeInput extends Input[LexRange] {
    def encode(data: LexRange): Chunk[Byte] = ???
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): Chunk[Byte] = ???
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): Chunk[Byte] = ???
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): Chunk[Byte] = ???
  }

  case object MemberScoreInput extends Input[MemberScore] {
    def encode(data: MemberScore): Chunk[Byte] = ???
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): Chunk[Byte] = ???
  }

  final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): Chunk[Byte] = ???
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): Chunk[Byte] = ???
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): Chunk[Byte] = ???
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): Chunk[Byte] = ???
  }

  case object RegexInput extends Input[Regex] {
    def encode(data: Regex): Chunk[Byte] = ???
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): Chunk[Byte] = ???
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): Chunk[Byte] = ???
  }

  case object ScoreRangeInput extends Input[ScoreRange] {
    def encode(data: ScoreRange): Chunk[Byte] = ???
  }

  case object StringInput extends Input[String] {
    def encode(data: String): Chunk[Byte] = ???
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): Chunk[Byte] = ???
  }

  case object TimeInput extends Input[Instant] {
    def encode(data: Instant): Chunk[Byte] = ???
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): Chunk[Byte] = ???
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): Chunk[Byte] = ???
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): Chunk[Byte] = ???
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): Chunk[Byte] = ???
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): Chunk[Byte] = ???
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): Chunk[Byte] = ???
  }

  final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): Chunk[Byte] = ???
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): Chunk[Byte] = ???
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): Chunk[Byte] = ???
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): Chunk[Byte] = ???
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): Chunk[Byte] = ???
  }
}
