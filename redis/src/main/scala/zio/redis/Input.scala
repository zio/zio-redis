/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis

import zio._
import zio.schema.codec.BinaryCodec

import java.time.Instant
import java.util.concurrent.TimeUnit

private[redis] sealed trait Input[-A] { self =>
  def encode(data: A): RespCommand

  final def contramap[B](f: B => A): Input[B] =
    new Input[B] {
      def encode(data: B): RespCommand = self.encode(f(data))
    }
}

private[redis] object Input {

  def apply[A](implicit input: Input[A]): Input[A] = input

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object AddressInput extends Input[Address] {
    def encode(data: Address): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): RespCommand =
      RespCommand(RespCommandArgument.Literal("AGGREGATE"), RespCommandArgument.Literal(data.stringify))
  }

  case object AlphaInput extends Input[Alpha] {
    def encode(data: Alpha): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object AuthInput extends Input[Auth] {
    import Utf8Codec.codec

    def encode(data: Auth): RespCommand =
      data.username match {
        case Some(username) =>
          RespCommand(RespCommandArgument.Value(username), RespCommandArgument.Value(data.password))
        case None => RespCommand(RespCommandArgument.Value(data.password))
      }

  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): RespCommand =
      RespCommand(RespCommandArgument.Literal(if (data) "1" else "0"))
  }

  case object StralgoLcsQueryTypeInput extends Input[StrAlgoLcsQueryType] {
    def encode(data: StrAlgoLcsQueryType): RespCommand = data match {
      case StrAlgoLcsQueryType.Len => RespCommand(RespCommandArgument.Literal("LEN"))
      case StrAlgoLcsQueryType.Idx(minMatchLength, withMatchLength) => {
        val idx = Chunk.single(RespCommandArgument.Literal("IDX"))
        val min =
          if (minMatchLength > 1)
            Chunk(RespCommandArgument.Literal("MINMATCHLEN"), RespCommandArgument.Unknown(minMatchLength.toString))
          else Chunk.empty[RespCommandArgument]
        val length =
          if (withMatchLength) Chunk.single(RespCommandArgument.Literal("WITHMATCHLEN"))
          else Chunk.empty[RespCommandArgument]
        RespCommand(Chunk(idx, min, length).flatten)
      }
    }
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand): RespCommand = {
      import BitFieldCommand._

      val respArgs = data match {
        case BitFieldGet(t, o) =>
          Chunk(
            RespCommandArgument.Literal("GET"),
            RespCommandArgument.Unknown(t.stringify),
            RespCommandArgument.Unknown(o.toString)
          )
        case BitFieldSet(t, o, v) =>
          Chunk(
            RespCommandArgument.Literal("SET"),
            RespCommandArgument.Unknown(t.stringify),
            RespCommandArgument.Unknown(o.toString),
            RespCommandArgument.Unknown(v.toString)
          )
        case BitFieldIncr(t, o, i) =>
          Chunk(
            RespCommandArgument.Literal("INCRBY"),
            RespCommandArgument.Unknown(t.stringify),
            RespCommandArgument.Unknown(o.toString),
            RespCommandArgument.Unknown(i.toString)
          )
        case bfo: BitFieldOverflow =>
          Chunk(RespCommandArgument.Literal("OVERFLOW"), RespCommandArgument.Literal(bfo.stringify))
      }
      RespCommand(respArgs)
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): RespCommand = {
      val start    = RespCommandArgument.Unknown(data.start.toString)
      val respArgs = data.end.fold(Chunk.single(start))(end => Chunk(start, RespCommandArgument.Unknown(end.toString)))
      RespCommand(respArgs)
    }
  }

  case object ByInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Literal("BY"), RespCommandArgument.Unknown(data))
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object ClientKillInput extends Input[ClientKillFilter] {
    def encode(data: ClientKillFilter): RespCommand = data match {
      case addr: ClientKillFilter.Address =>
        RespCommand(RespCommandArgument.Literal("ADDR"), RespCommandArgument.Unknown(addr.stringify))
      case laddr: ClientKillFilter.LocalAddress =>
        RespCommand(RespCommandArgument.Literal("LADDR"), RespCommandArgument.Unknown(laddr.stringify))
      case ClientKillFilter.Id(clientId) =>
        RespCommand(RespCommandArgument.Literal("ID"), RespCommandArgument.Unknown(clientId.toString))
      case ClientKillFilter.Type(clientType) =>
        RespCommand(RespCommandArgument.Literal("TYPE"), RespCommandArgument.Literal(clientType.stringify))
      case ClientKillFilter.User(username) =>
        RespCommand(RespCommandArgument.Literal("USER"), RespCommandArgument.Unknown(username))
      case ClientKillFilter.SkipMe(skip) =>
        RespCommand(RespCommandArgument.Literal("SKIPME"), RespCommandArgument.Literal(if (skip) "YES" else "NO"))
    }
  }

  case object ClientPauseModeInput extends Input[ClientPauseMode] {
    def encode(data: ClientPauseMode): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object ClientTrackingInput
      extends Input[Option[(Option[Long], Option[ClientTrackingMode], Boolean, Chunk[String])]] {
    def encode(
      data: Option[(Option[Long], Option[ClientTrackingMode], Boolean, Chunk[String])]
    ): RespCommand =
      data match {
        case Some((clientRedir, mode, noLoop, prefixes)) =>
          val modeChunk = mode match {
            case Some(ClientTrackingMode.OptIn)     => RespCommand(RespCommandArgument.Literal("OPTIN"))
            case Some(ClientTrackingMode.OptOut)    => RespCommand(RespCommandArgument.Literal("OPTOUT"))
            case Some(ClientTrackingMode.Broadcast) => RespCommand(RespCommandArgument.Literal("BCAST"))
            case None                               => RespCommand.empty
          }
          val loopChunk = if (noLoop) RespCommand(RespCommandArgument.Literal("NOLOOP")) else RespCommand.empty
          RespCommand(RespCommandArgument.Literal("ON")) ++
            clientRedir.fold(RespCommand.empty)(id =>
              RespCommand(RespCommandArgument.Literal("REDIRECT"), RespCommandArgument.Unknown(id.toString))
            ) ++
            RespCommand(
              prefixes.flatMap(prefix =>
                Chunk(RespCommandArgument.Literal("PREFIX"), RespCommandArgument.Unknown(prefix))
              )
            ) ++
            modeChunk ++
            loopChunk
        case None =>
          RespCommand(RespCommandArgument.Literal("OFF"))
      }
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): RespCommand =
      RespCommand(RespCommandArgument.Literal("COUNT"), RespCommandArgument.Unknown(data.count.toString))
  }

  case object RedisTypeInput extends Input[RedisType] {
    def encode(data: RedisType): RespCommand =
      RespCommand(RespCommandArgument.Literal("TYPE"), RespCommandArgument.Literal(data.stringify))
  }

  case object PatternInput extends Input[Pattern] {
    def encode(data: Pattern): RespCommand =
      RespCommand(RespCommandArgument.Literal("MATCH"), RespCommandArgument.Unknown(data.pattern))
  }

  case object GetInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Literal("GET"), RespCommandArgument.Unknown(data))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object SideInput extends Input[Side] {
    def encode(data: Side): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommand = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      RespCommand(RespCommandArgument.Unknown(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration): RespCommand = {
      val milliseconds = data.toMillis
      RespCommand(RespCommandArgument.Literal("PX"), RespCommandArgument.Unknown(milliseconds.toString))
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq): RespCommand =
      RespCommand(RespCommandArgument.Literal("FREQ"), RespCommandArgument.Unknown(data.frequency))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime): RespCommand =
      RespCommand(RespCommandArgument.Literal("IDLETIME"), RespCommandArgument.Unknown(data.seconds.toString))
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("LIMIT"),
        RespCommandArgument.Unknown(data.offset.toString),
        RespCommandArgument.Unknown(data.count.toString)
      )
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): RespCommand =
      RespCommand(
        RespCommandArgument.Unknown(data.longitude.toString),
        RespCommandArgument.Unknown(data.latitude.toString)
      )
  }

  final case class MemberScoreInput[M: BinaryCodec]() extends Input[MemberScore[M]] {
    def encode(data: MemberScore[M]): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.score.toString), RespCommandArgument.Value(data.member))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): RespCommand = RespCommand.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): RespCommand =
      (data._1 :: data._2).foldLeft(RespCommand.empty)((acc, a) => acc ++ input.encode(a))
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.start.toString), RespCommandArgument.Unknown(data.end.toString))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): RespCommand =
      RespCommand(RespCommandArgument.Literal("STOREDIST"), RespCommandArgument.Unknown(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): RespCommand =
      RespCommand(RespCommandArgument.Literal("STORE"), RespCommandArgument.Unknown(data.key))
  }

  case object StringInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data))
  }

  case object CommandNameInput extends Input[String] {
    def encode(data: String): RespCommand =
      RespCommand(RespCommandArgument.CommandName(data))
  }

  final case class ArbitraryValueInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommand =
      RespCommand(RespCommandArgument.Value(data))
  }

  final case class ArbitraryKeyInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommand =
      RespCommand(RespCommandArgument.Key(data))
  }

  case object ValueInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): RespCommand =
      RespCommand(RespCommandArgument.Value(data))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): RespCommand =
      data.fold(RespCommand.empty)(a.encode)
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.getEpochSecond.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.toEpochMilli.toString))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double]): RespCommand = {
      val args = data.foldLeft(Chunk.single[RespCommandArgument](RespCommandArgument.Literal("WEIGHTS"))) { (acc, a) =>
        acc ++ Chunk.single(RespCommandArgument.Unknown(a.toString))
      }
      RespCommand(args)
    }
  }

  case object IdleInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("IDLE"), RespCommandArgument.Unknown(data.toMillis.toString))
  }

  case object TimeInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("TIME"), RespCommandArgument.Unknown(data.toMillis.toString))
  }

  case object RetryCountInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Literal("RETRYCOUNT"), RespCommandArgument.Unknown(data.toString))
  }

  final case class XGroupCreateInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.Create[K, G, I]] {
    def encode(data: XGroupCommand.Create[K, G, I]): RespCommand = {
      val chunk = Chunk(
        RespCommandArgument.Literal("CREATE"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Unknown(data.group),
        RespCommandArgument.Unknown(data.id)
      )

      RespCommand(if (data.mkStream) chunk :+ RespCommandArgument.Literal(MkStream.stringify) else chunk)
    }
  }

  final case class XGroupSetIdInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.SetId[K, G, I]] {
    def encode(data: XGroupCommand.SetId[K, G, I]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("SETID"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Unknown(data.group),
        RespCommandArgument.Unknown(data.id)
      )
  }

  final case class XGroupDestroyInput[K: BinaryCodec, G: BinaryCodec]() extends Input[XGroupCommand.Destroy[K, G]] {
    def encode(data: XGroupCommand.Destroy[K, G]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("DESTROY"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Unknown(data.group)
      )
  }

  final case class XGroupCreateConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.CreateConsumer[K, G, C]] {
    def encode(data: XGroupCommand.CreateConsumer[K, G, C]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("CREATECONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Unknown(data.group),
        RespCommandArgument.Unknown(data.consumer)
      )
  }

  final case class XGroupDelConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.DelConsumer[K, G, C]] {
    def encode(data: XGroupCommand.DelConsumer[K, G, C]): RespCommand =
      RespCommand(
        RespCommandArgument.Literal("DELCONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Unknown(data.group),
        RespCommandArgument.Unknown(data.consumer)
      )
  }

  case object BlockInput extends Input[Duration] {
    def encode(data: Duration): RespCommand =
      RespCommand(RespCommandArgument.Literal("BLOCK"), RespCommandArgument.Unknown(data.toMillis.toString))
  }

  final case class StreamsInput[K: BinaryCodec, V: BinaryCodec]() extends Input[((K, V), Chunk[(K, V)])] {
    def encode(data: ((K, V), Chunk[(K, V)])): RespCommand = {
      val (keys, ids) = (data._1 +: data._2).map { case (key, value) =>
        (RespCommandArgument.Key(key), RespCommandArgument.Value(value))
      }.unzip

      RespCommand(Chunk.single(RespCommandArgument.Literal("STREAMS")) ++ keys ++ ids)
    }
  }

  case object NoAckInput extends Input[NoAck] {
    def encode(data: NoAck): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  case object StreamMaxLenInput extends Input[StreamMaxLen] {
    def encode(data: StreamMaxLen): RespCommand = {
      val chunk =
        if (data.approximate) Chunk(RespCommandArgument.Literal("MAXLEN"), RespCommandArgument.Literal("~"))
        else Chunk.single(RespCommandArgument.Literal("MAXLEN"))

      RespCommand(chunk :+ RespCommandArgument.Unknown(data.count.toString))
    }
  }

  case object ListMaxLenInput extends Input[ListMaxLen] {
    def encode(data: ListMaxLen): RespCommand =
      RespCommand(RespCommandArgument.Literal("MAXLEN"), RespCommandArgument.Unknown(data.count.toString))
  }

  case object RankInput extends Input[Rank] {
    def encode(data: Rank): RespCommand =
      RespCommand(RespCommandArgument.Literal("RANK"), RespCommandArgument.Unknown(data.rank.toString))
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): RespCommand =
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
    def encode(data: (A, B, C, D, E, F)): RespCommand =
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
    def encode(data: (A, B, C, D, E, F, G)): RespCommand =
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
    def encode(data: (A, B, C, D, E, F, G, H, I)): RespCommand =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J)): RespCommand =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): RespCommand =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  final case class GetExPersistInput[K: BinaryCodec]() extends Input[(K, Boolean)] {
    def encode(data: (K, Boolean)): RespCommand =
      RespCommand(
        if (data._2) Chunk(RespCommandArgument.Key(data._1), RespCommandArgument.Literal("PERSIST"))
        else Chunk(RespCommandArgument.Key(data._1))
      )
  }

  final case class GetExInput[K: BinaryCodec]() extends Input[(K, Expire, Duration)] {
    def encode(data: (K, Expire, Duration)): RespCommand =
      data match {
        case (key, Expire.SetExpireSeconds, duration) =>
          RespCommand(RespCommandArgument.Key(key), RespCommandArgument.Literal("EX")) ++ DurationSecondsInput.encode(
            duration
          )
        case (key, Expire.SetExpireMilliseconds, duration) =>
          RespCommand(RespCommandArgument.Key(key), RespCommandArgument.Literal("PX")) ++ DurationMillisecondsInput
            .encode(duration)
      }
  }

  final case class GetExAtInput[K: BinaryCodec]() extends Input[(K, ExpiredAt, Instant)] {
    def encode(data: (K, ExpiredAt, Instant)): RespCommand =
      data match {
        case (key, ExpiredAt.SetExpireAtSeconds, instant) =>
          RespCommand(RespCommandArgument.Key(key), RespCommandArgument.Literal("EXAT")) ++ TimeSecondsInput.encode(
            instant
          )
        case (key, ExpiredAt.SetExpireAtMilliseconds, instant) =>
          RespCommand(RespCommandArgument.Key(key), RespCommandArgument.Literal("PXAT")) ++ TimeMillisecondsInput
            .encode(instant)
      }
  }

  case object IdInput extends Input[Long] {
    def encode(data: Long): RespCommand =
      RespCommand(RespCommandArgument.Literal("ID"), RespCommandArgument.Unknown(data.toString))
  }

  case object UnblockBehaviorInput extends Input[UnblockBehavior] {
    def encode(data: UnblockBehavior): RespCommand =
      RespCommand(RespCommandArgument.Unknown(data.stringify))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): RespCommand =
      data.foldLeft(RespCommand.empty)((acc, a) => acc ++ input.encode(a))
  }

  final case class EvalInput[-K, -V](inputK: Input[K], inputV: Input[V]) extends Input[(String, Chunk[K], Chunk[V])] {
    def encode(data: (String, Chunk[K], Chunk[V])): RespCommand = {
      val (lua, keys, args) = data
      val encodedScript     = RespCommand(RespCommandArgument.Unknown(lua), RespCommandArgument.Unknown(keys.size.toString))
      val encodedKeys = keys.foldLeft(RespCommand.empty)((acc, a) =>
        acc ++ inputK.encode(a).mapArguments(arg => RespCommandArgument.Key(arg.value.value))
      )
      val encodedArgs = args.foldLeft(RespCommand.empty)((acc, a) =>
        acc ++ inputV.encode(a).mapArguments(arg => RespCommandArgument.Value(arg.value.value))
      )
      encodedScript ++ encodedKeys ++ encodedArgs
    }
  }

  case object ScriptDebugInput extends Input[DebugMode] {
    def encode(data: DebugMode): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object ScriptFlushInput extends Input[FlushMode] {
    def encode(data: FlushMode): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithForceInput extends Input[WithForce] {
    def encode(data: WithForce): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object WithJustIdInput extends Input[WithJustId] {
    def encode(data: WithJustId): RespCommand =
      RespCommand(RespCommandArgument.Literal(data.stringify))
  }

  case object YesNoInput extends Input[Boolean] {
    def encode(data: Boolean): RespCommand =
      RespCommand(RespCommandArgument.Literal(if (data) "YES" else "NO"))
  }
}
