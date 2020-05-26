package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A]

object Input {
  case object AbsTtlInput           extends Input[AbsTtl]
  case object AggregateInput        extends Input[Aggregate]
  case object AuthInput             extends Input[Auth]
  case object BoolInput             extends Input[Boolean]
  case object BitFieldGetInput      extends Input[BitFieldGet]
  case object BitFieldSetInput      extends Input[BitFieldSet]
  case object BitFieldIncrInput     extends Input[BitFieldIncr]
  case object BitFieldOverflowInput extends Input[BitFieldOverflow]
  case object BitOperationInput     extends Input[BitOperation]
  case object BitPosRangeInput      extends Input[BitPosRange]
  case object ByteInput             extends Input[Chunk[Byte]]
  case object ChangedInput          extends Input[Changed]
  case object CopyInput             extends Input[Copy]
  case object CountInput            extends Input[Count]
  case object DoubleInput           extends Input[Double]
  case object DurationInput         extends Input[Duration]
  case object FreqInput             extends Input[Freq]
  case object IdleTimeInput         extends Input[IdleTime]
  case object IncrementInput        extends Input[Increment]
  case object KeepTtlInput          extends Input[KeepTtl]
  case object LexRangeInput         extends Input[LexRange]
  case object LimitInput            extends Input[Limit]
  case object LongInput             extends Input[Long]
  case object LongLatInput          extends Input[LongLat]
  case object MemberScoreInput      extends Input[MemberScore]
  case object NoInput               extends Input[Unit]

  final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])]

  case object OrderInput      extends Input[Order]
  case object RadiusUnitInput extends Input[RadiusUnit]
  case object RangeInput      extends Input[Range]
  case object RegexInput      extends Input[Regex]
  case object ReplaceInput    extends Input[Replace]

  case object StoreDistInput  extends Input[StoreDist]
  case object StoreInput      extends Input[Store]
  case object ScoreRangeInput extends Input[ScoreRange]
  case object StringInput     extends Input[String]

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]]

  case object TimeInput extends Input[Instant]

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)]

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)]

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)]

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
  ) extends Input[(A, B, C, D, E, F, G, H, I, J, K)]

  case object UpdateInput extends Input[Update]

  final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]

  case object WithScoresInput extends Input[WithScores]
  case object WithCoordInput  extends Input[WithCoord]
  case object WithDistInput   extends Input[WithDist]
  case object WithHashInput   extends Input[WithHash]
}
