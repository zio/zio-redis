package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A]

object Input {
  case object ByteInput                 extends Input[Chunk[Byte]]
  case object DoubleInput               extends Input[Double]
  case object DurationInput             extends Input[Duration]
  case object LongInput                 extends Input[Long]
  case object MatchInput                extends Input[Regex]
  case object UnitInput                 extends Input[Unit]
  case object RangeInput                extends Input[Range]
  case object StringInput               extends Input[String]
  case object TimeInput                 extends Input[Instant]
  case object SortedSetMemberScoreInput extends Input[options.MemberScore]
  case object SortedSetUpdateInput      extends Input[options.Updates]
  case object SortedSetChangedInput     extends Input[options.CH]
  case object SortedSetIncrementInput   extends Input[options.INCR]
  case object SortedSetAggregateInput   extends Input[options.Aggregate]
  case object SortedSetLexRangeInput    extends Input[options.LexRange]
  case object SortedSetWithScoresInput  extends Input[options.WithScores]
  case object SortedSetLimitInput       extends Input[options.Limit]
  case object SortedSetScoreRangeInput  extends Input[options.ScoreRange]

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]]

  final case class NonEmptyList[-A](a: Input[A]) extends Input[(A, List[A])]

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)]

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)]

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)]

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)]

  final case class Tuple6[-A, -B, -C, -D, -E, -F](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F]
  ) extends Input[(A, B, C, D, E, F)]

  final case class Varargs[-A](value: Input[A]) extends Input[Iterable[A]]
}
