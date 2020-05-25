package zio.redis

import java.time.Instant

import zio.Chunk
import zio.duration.Duration

import scala.util.matching.Regex

sealed trait Input[-A] {
  private[redis] def encode(data: A): Chunk[String]
}

object Input {
  private[this] def wrap(text: String): String = s"$$${text.length}\r\n$text\r\n"

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): Chunk[String] = Chunk.single(wrap(if (data) "1" else "0"))
  }

  case object BitFieldGetInput extends Input[BitFieldGet] {
    def encode(data: BitFieldGet): Chunk[String] =
      Chunk(wrap("GET"), wrap(data.`type`.stringify), wrap(data.offset.toString))
  }

  case object BitFieldSetInput extends Input[BitFieldSet] {
    def encode(data: BitFieldSet): Chunk[String] =
      Chunk(wrap("SET"), wrap(data.`type`.stringify), wrap(data.offset.toString), wrap(data.value.toString))
  }

  case object BitFieldIncrInput extends Input[BitFieldIncr] {
    def encode(data: BitFieldIncr): Chunk[String] =
      Chunk(wrap("INCRBY"), wrap(data.`type`.stringify), wrap(data.offset.toString), wrap(data.increment.toString))
  }

  case object BitFieldOverflowInput extends Input[BitFieldOverflow] {
    def encode(data: BitFieldOverflow): Chunk[String] = Chunk(wrap("OVERFLOW"), wrap(data.stringify))
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): Chunk[String] = ???
  }

  case object ByteInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): Chunk[String] = ???
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): Chunk[String] = ???
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): Chunk[String] = Chunk.single(wrap(data.toString))
  }

  case object DurationInput extends Input[Duration] {
    def encode(data: Duration): Chunk[String] = ???
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): Chunk[String] = Chunk.single(wrap(data.stringify))
  }

  case object LexRangeInput extends Input[LexRange] {
    def encode(data: LexRange): Chunk[String] = ???
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): Chunk[String] = ???
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): Chunk[String] = Chunk.single(wrap(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): Chunk[String] = ???
  }

  case object MemberScoreInput extends Input[MemberScore] {
    def encode(data: MemberScore): Chunk[String] = ???
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): Chunk[String] = Chunk.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): Chunk[String] =
      (data._1 :: data._2).foldLeft(Chunk.empty: Chunk[String])((acc, a) => acc ++ input.encode(a))
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
    def encode(data: StoreDist): Chunk[String] = Chunk(wrap("STOREDIST"), wrap(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): Chunk[String] = Chunk(wrap("STORE"), wrap(data.key))
  }

  case object ScoreRangeInput extends Input[ScoreRange] {
    def encode(data: ScoreRange): Chunk[String] = ???
  }

  case object StringInput extends Input[String] {
    def encode(data: String): Chunk[String] = Chunk.single(wrap(data))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): Chunk[String] = ???
  }

  case object TimeInput extends Input[Instant] {
    def encode(data: Instant): Chunk[String] = ???
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
