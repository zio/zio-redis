package zio.redis

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.util.matching.Regex

import zio.Chunk
import zio.duration.Duration

sealed trait Input[-A] {
  private[redis] def encode(data: A): Chunk[RespValue.BulkString]
}

object Input {

  @inline
  private[this] def stringEncode(s: String) = RespValue.bulkString(s)

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("AGGREGATE"), stringEncode(data.stringify))
  }

  case object AuthInput extends Input[Auth] {
    def encode(data: Auth): Chunk[RespValue.BulkString] = Chunk(stringEncode("AUTH"), stringEncode(data.password))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(if (data) "1" else "0"))
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand): Chunk[RespValue.BulkString] = {
      import BitFieldCommand._

      data match {
        case BitFieldGet(t, o) => Chunk(stringEncode("GET"), stringEncode(t.stringify), stringEncode(o.toString))
        case BitFieldSet(t, o, v) =>
          Chunk(stringEncode("SET"), stringEncode(t.stringify), stringEncode(o.toString), stringEncode(v.toString))
        case BitFieldIncr(t, o, i) =>
          Chunk(stringEncode("INCRBY"), stringEncode(t.stringify), stringEncode(o.toString), stringEncode(i.toString))
        case bfo: BitFieldOverflow => Chunk(stringEncode("OVERFLOW"), stringEncode(bfo.stringify))
      }
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): Chunk[RespValue.BulkString] = {
      val start = stringEncode(data.start.toString)
      data.end.fold(Chunk.single(start))(end => Chunk(start, stringEncode(end.toString)))
    }
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("COUNT"), stringEncode(data.count.toString))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      Chunk.single(stringEncode(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] = {
      val milliseconds = data.toMillis
      Chunk(stringEncode("PX"), stringEncode(milliseconds.toString))
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq): Chunk[RespValue.BulkString] = Chunk(stringEncode("FREQ"), stringEncode(data.frequency))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("IDLETIME"), stringEncode(data.seconds.toString))
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object LexRangeInput extends Input[LexRange] {
    def encode(data: LexRange): Chunk[RespValue.BulkString] =
      Chunk(stringEncode(data.min.stringify), stringEncode(data.max.stringify))
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("LIMIT"), stringEncode(data.offset.toString), stringEncode(data.count.toString))
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): Chunk[RespValue.BulkString] =
      Chunk(stringEncode(data.longitude.toString), stringEncode(data.latitude.toString))
  }

  case object MemberScoreInput extends Input[MemberScore] {
    def encode(data: MemberScore): Chunk[RespValue.BulkString] =
      Chunk(stringEncode(data.score.toString), stringEncode(data.member))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): Chunk[RespValue.BulkString] = Chunk.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): Chunk[RespValue.BulkString] =
      (data._1 :: data._2).foldLeft(Chunk.empty: Chunk[RespValue.BulkString])((acc, a) => acc ++ input.encode(a))
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): Chunk[RespValue.BulkString] =
      Chunk(stringEncode(data.start.toString), stringEncode(data.end.toString))
  }

  case object RegexInput extends Input[Regex] {
    def encode(data: Regex): Chunk[RespValue.BulkString] = Chunk(stringEncode("MATCH"), stringEncode(data.regex))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): Chunk[RespValue.BulkString] = Chunk(stringEncode("STOREDIST"), stringEncode(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): Chunk[RespValue.BulkString] = Chunk(stringEncode("STORE"), stringEncode(data.key))
  }

  case object ScoreRangeInput extends Input[ScoreRange] {
    def encode(data: ScoreRange): Chunk[RespValue.BulkString] =
      Chunk(stringEncode(data.min.stringify), stringEncode(data.max.stringify))
  }

  case object StringInput extends Input[String] {
    def encode(data: String): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data))
  }

  case object ByteInput extends Input[Chunk[Byte]] {
    override private[redis] def encode(data: Chunk[Byte]) = Chunk.single(RespValue.BulkString(data))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): Chunk[RespValue.BulkString] =
      data.fold(Chunk.empty: Chunk[RespValue.BulkString])(a.encode)
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.getEpochSecond.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.toEpochMilli.toString))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double]): Chunk[RespValue.BulkString] =
      data.foldLeft(Chunk.single(stringEncode("WEIGHTS")): Chunk[RespValue.BulkString])((acc, a) =>
        acc ++ Chunk.single(stringEncode(a.toString))
      )
  }

  case object IdleInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("IDLE"), stringEncode(data.toMillis.toString))
  }

  case object TimeInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("TIME"), stringEncode(data.toMillis.toString))
  }

  case object RetryCountInput extends Input[Long] {
    def encode(data: Long): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("RETRYCOUNT"), stringEncode(data.toString))
  }

  case object XGroupCreateInput extends Input[XGroupCommand.Create] {
    def encode(data: XGroupCommand.Create): Chunk[RespValue.BulkString] = {
      val chunk =
        Chunk(
          stringEncode("CREATE"),
          stringEncode(data.key),
          stringEncode(data.group),
          stringEncode(data.id)
        )

      if (data.mkStream)
        chunk :+ stringEncode(MkStream.stringify)
      else
        chunk
    }
  }

  case object XGroupSetIdInput extends Input[XGroupCommand.SetId] {
    def encode(data: XGroupCommand.SetId): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("SETID"), stringEncode(data.key), stringEncode(data.group), stringEncode(data.id))
  }

  case object XGroupDestroyInput extends Input[XGroupCommand.Destroy] {
    def encode(data: XGroupCommand.Destroy): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("DESTROY"), stringEncode(data.key), stringEncode(data.group))
  }

  case object XGroupCreateConsumerInput extends Input[XGroupCommand.CreateConsumer] {
    def encode(data: XGroupCommand.CreateConsumer): Chunk[RespValue.BulkString] =
      Chunk(
        stringEncode("CREATECONSUMER"),
        stringEncode(data.key),
        stringEncode(data.group),
        stringEncode(data.consumer)
      )
  }

  case object XGroupDelConsumerInput extends Input[XGroupCommand.DelConsumer] {
    def encode(data: XGroupCommand.DelConsumer): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("DELCONSUMER"), stringEncode(data.key), stringEncode(data.group), stringEncode(data.consumer))
  }

  case object BlockInput extends Input[Duration] {
    def encode(data: Duration): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("BLOCK"), stringEncode(data.toMillis.toString))
  }

  case object StreamsInput extends Input[((String, String), Chunk[(String, String)])] {
    def encode(data: ((String, String), Chunk[(String, String)])): Chunk[RespValue.BulkString] = {
      val (keys, ids) =
        (data._1 +: data._2).map(pair => (stringEncode(pair._1), stringEncode(pair._2))).unzip

      Chunk.single(stringEncode("STREAMS")) ++ keys ++ ids
    }
  }

  case object GroupInput extends Input[Group] {
    def encode(data: Group): Chunk[RespValue.BulkString] =
      Chunk(stringEncode("GROUP"), stringEncode(data.group), stringEncode(data.consumer))
  }

  case object NoAckInput extends Input[NoAck] {
    def encode(data: NoAck): Chunk[RespValue.BulkString] =
      Chunk.single(stringEncode(data.stringify))
  }

  case object MaxLenInput extends Input[MaxLen] {
    def encode(data: MaxLen): Chunk[RespValue.BulkString] = {
      val chunk =
        if (data.approximate)
          Chunk(stringEncode("MAXLEN"), stringEncode("~"))
        else
          Chunk.single(stringEncode("MAXLEN"))

      chunk :+ stringEncode(data.count.toString)
    }
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): Chunk[RespValue.BulkString] = _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5)
  }

  final case class Tuple6[-A, -B, -C, -D, -E, -F](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F]
  ) extends Input[(A, B, C, D, E, F)] {
    def encode(data: (A, B, C, D, E, F)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6)
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
    def encode(data: (A, B, C, D, E, F, G)): Chunk[RespValue.BulkString] =
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
    def encode(data: (A, B, C, D, E, F, G, H, I)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++ _5.encode(data._5) ++
        _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++ _9.encode(data._9)
  }

  final case class Tuple10[-A, -B, -C, -D, -E, -F, -G, -H, -I, -J](
    _1: Input[A],
    _2: Input[B],
    _3: Input[C],
    _4: Input[D],
    _5: Input[E],
    _6: Input[F],
    _7: Input[G],
    _8: Input[H],
    _9: Input[I],
    _10: Input[J]
  ) extends Input[(A, B, C, D, E, F, G, H, I, J)] {
    def encode(data: (A, B, C, D, E, F, G, H, I, J)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10)
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): Chunk[RespValue.BulkString] =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): Chunk[RespValue.BulkString] =
      data.foldLeft(Chunk.empty: Chunk[RespValue.BulkString])((acc, a) => acc ++ input.encode(a))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object WithForceInput extends Input[WithForce] {
    def encode(data: WithForce): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }

  case object WithJustIdInput extends Input[WithJustId] {
    def encode(data: WithJustId): Chunk[RespValue.BulkString] = Chunk.single(stringEncode(data.stringify))
  }
}
