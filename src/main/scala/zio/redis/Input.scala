package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A]

object Input {
  case object BoolInput             extends Input[Boolean]
  case object ByteInput             extends Input[Chunk[Byte]]
  case object DoubleInput           extends Input[Double]
  case object DurationInput         extends Input[Duration]
  case object LongInput             extends Input[Long]
  case object MatchInput            extends Input[Regex]
  case object UnitInput             extends Input[Unit]
  case object RangeInput            extends Input[Range]
  case object StringInput           extends Input[String]
  case object TimeInput             extends Input[Instant]
  case object MemberScoreInput      extends Input[MemberScore]
  case object ChangedInput          extends Input[Change]
  case object IncrementInput        extends Input[Increment]
  case object AggregateInput        extends Input[Aggregate]
  case object LexRangeInput         extends Input[LexRange]
  case object WithScoresInput       extends Input[WithScores]
  case object LimitInput            extends Input[Limit]
  case object ScoreRangeInput       extends Input[ScoreRange]
  case object CountInput            extends Input[Count]
  case object LongLatInput          extends Input[LongLat]
  case object OrderInput            extends Input[Order]
  case object RadiusUnitInput       extends Input[RadiusUnit]
  case object StoreInput            extends Input[Store]
  case object StoreDistInput        extends Input[StoreDist]
  case object WithCoordInput        extends Input[WithCoord]
  case object WithDistInput         extends Input[WithDist]
  case object WithHashInput         extends Input[WithHash]
  case object BitFieldGetInput      extends Input[BitFieldGet]
  case object BitFieldSetInput      extends Input[BitFieldSet]
  case object BitFieldIncrInput     extends Input[BitFieldIncr]
  case object BitFieldOverflowInput extends Input[BitFieldOverflow]
  case object BitOperationInput     extends Input[BitOperation]
  case object BitPosRangeInput      extends Input[BitPosRange]
  case object KeepTtlInput          extends Input[KeepTtl]
  case object UpdateInput           extends Input[Update]

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]]

  final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])]

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)]

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)]

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)]

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
  ) extends Input[(A, B, C, D, E, F, G, H, I, J, K)]

  final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]
}
