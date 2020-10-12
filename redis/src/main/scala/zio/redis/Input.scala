package zio.redis

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.util.matching.Regex

import zio.Chunk
import zio.duration.Duration

sealed trait Input[-A] {
  private[redis] def encode(data: A): Chunk[String]
}

object Input {
  private[this] def wrap(text: String): String = s"$$${text.length}\r\n$text\r\n"

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): Chunk[String] = Chunk(wrap("AGGREGATE"), wrap(data.stringify))
  }

  case object AuthInput extends Input[Auth] {
    def encode(data: Auth): Chunk[String] = Chunk(wrap("AUTH"), wrap(data.password))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): Chunk[String] = Chunk.single(wrap(if (data) "1" else "0"))
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand): Chunk[String] = {
      import BitFieldCommand._

      data match {
        case BitFieldGet(t, o)     => Chunk(wrap("GET"), wrap(t.stringify), wrap(o.toString))
        case BitFieldSet(t, o, v)  => Chunk(wrap("SET"), wrap(t.stringify), wrap(o.toString), wrap(v.toString))
        case BitFieldIncr(t, o, i) => Chunk(wrap("INCRBY"), wrap(t.stringify), wrap(o.toString), wrap(i.toString))
        case bfo: BitFieldOverflow => Chunk(wrap("OVERFLOW"), wrap(bfo.stringify))
      }
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): Chunk[String] = {
      val start = wrap(data.start.toString)
      data.end.fold(Chunk.single(start))(end => Chunk(start, wrap(end.toString)))
    }
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): Chunk[String] = Chunk(wrap("COUNT"), wrap(data.count.toString))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): Chunk[String] = Chunk.single(wrap(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration): Chunk[String] = Chunk.single(wrap(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration): Chunk[String] = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      Chunk.single(wrap(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration): Chunk[String] = {
      val milliseconds = data.toMillis
      Chunk(wrap("PX"), wrap(milliseconds.toString))
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq): Chunk[String] = Chunk(wrap("FREQ"), wrap(data.frequency))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime): Chunk[String] = Chunk(wrap("IDLETIME"), wrap(data.seconds.toString))
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object LexRangeInput extends Input[LexRange] {
    def encode(data: LexRange): Chunk[String] = Chunk(wrap(data.min.stringify), wrap(data.max.stringify))
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): Chunk[String] =
      Chunk(wrap("LIMIT"), wrap(data.offset.toString), wrap(data.count.toString))
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): Chunk[String] = Chunk.single(wrap(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): Chunk[String] = Chunk(wrap(data.longitude.toString), wrap(data.latitude.toString))
  }

  case object MemberScoreInput extends Input[MemberScore] {
    def encode(data: MemberScore): Chunk[String] = Chunk(wrap(data.score.toString), wrap(data.member))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): Chunk[String] = Chunk.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): Chunk[String] =
      (data._1 :: data._2).foldLeft(Chunk.empty: Chunk[String])((acc, a) => acc ++ input.encode(a))
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): Chunk[String] = Chunk(wrap(data.start.toString), wrap(data.end.toString))
  }

  case object RegexInput extends Input[Regex] {
    def encode(data: Regex): Chunk[String] = Chunk(wrap("MATCH"), wrap(data.regex))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): Chunk[String] = Chunk(wrap("STOREDIST"), wrap(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): Chunk[String] = Chunk(wrap("STORE"), wrap(data.key))
  }

  case object ScoreRangeInput extends Input[ScoreRange] {
    def encode(data: ScoreRange): Chunk[String] = Chunk(wrap(data.min.stringify), wrap(data.max.stringify))
  }

  case object StringInput extends Input[String] {
    def encode(data: String): Chunk[String] = Chunk.single(wrap(data))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): Chunk[String] = data.fold(Chunk.empty: Chunk[String])(a.encode)
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant): Chunk[String] = Chunk.single(wrap(data.getEpochSecond.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant): Chunk[String] = Chunk.single(wrap(data.toEpochMilli.toString))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double]): Chunk[String] =
      data.foldLeft(Chunk.single(wrap("WEIGHTS")): Chunk[String])((acc, a) => acc ++ Chunk.single(wrap(a.toString)))
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): Chunk[String] = _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): Chunk[String] = _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): Chunk[String] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): Chunk[String] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5)
  }

  final case class Tuple7[-A, -B, -C, -D, -E, -F, -G](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G]
  ) extends Input[(A, B, C, D, E, F, G)] {
    def encode(data: (A, B, C, D, E, F, G)): Chunk[String] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6) ++ _7.encode(data._7)
  }

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
  ) extends Input[(A, B, C, D, E, F, G, H, I)] {
    def encode(data: (A, B, C, D, E, F, G, H, I)): Chunk[String] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++ _9.encode(data._9)
  }

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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): Chunk[String] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): Chunk[String] =
      data.foldLeft(Chunk.empty: Chunk[String])((acc, a) => acc ++ input.encode(a))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): Chunk[String] = Chunk.single(wrap(data.stringify))
  }
}
