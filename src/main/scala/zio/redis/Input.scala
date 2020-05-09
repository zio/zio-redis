package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration
import zio.redis.options._

import scala.util.matching.Regex

sealed trait Input[-A]

object Input {
  case object BoolInput                   extends Input[Boolean]
  case object ByteInput                   extends Input[Chunk[Byte]]
  case object DoubleInput                 extends Input[Double]
  case object DurationInput               extends Input[Duration]
  case object LongInput                   extends Input[Long]
  case object MatchInput                  extends Input[Regex]
  case object UnitInput                   extends Input[Unit]
  case object RangeInput                  extends Input[Range]
  case object StringInput                 extends Input[String]
  case object TimeInput                   extends Input[Instant]
  case object SortedSetMemberScoreInput   extends Input[MemberScore]
  case object SortedSetUpdateInput        extends Input[Updates]
  case object SortedSetChangedInput       extends Input[CH]
  case object SortedSetIncrementInput     extends Input[INCR]
  case object SortedSetAggregateInput     extends Input[Aggregate]
  case object SortedSetLexRangeInput      extends Input[LexRange]
  case object SortedSetWithScoresInput    extends Input[WithScores]
  case object SortedSetLimitInput         extends Input[Limit]
  case object SortedSetScoreRangeInput    extends Input[ScoreRange]
  case object GeoCountInput               extends Input[Count]
  case object GeoLongLatInput             extends Input[LongLat]
  case object GeoOrderInput               extends Input[Order]
  case object GeoRadiusUnitInput          extends Input[RadiusUnit]
  case object GeoStoreInput               extends Input[Store]
  case object GeoStoreDistInput           extends Input[StoreDist]
  case object GeoWithCoordInput           extends Input[WithCoord]
  case object GeoWithDistInput            extends Input[WithDist]
  case object GeoWithHashInput            extends Input[WithHash]
  case object StringBitFieldGetInput      extends Input[BitFieldGet]
  case object StringBitFieldSetInput      extends Input[BitFieldSet]
  case object StringBitFieldIncrInput     extends Input[BitFieldIncr]
  case object StringBitFieldOverflowInput extends Input[BitFieldOverflow]
  case object StringBitOperationInput     extends Input[BitOperation]
  case object StringBitPosRangeInput      extends Input[BitPosRange]
  case object StringExpirationInput       extends Input[Expiration]
  case object StringKeepTimeToLiveInput   extends Input[KEEPTTL]
  case object UpdatesInput                extends Input[Updates]

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
