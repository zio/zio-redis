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
import zio.redis.internal.{RespCommandArguments, RespCommandArgument}
import zio.schema.codec.BinaryCodec

import java.time.Instant
import java.util.concurrent.TimeUnit

sealed trait Input[-A] { self =>
  private[redis] def encode(data: A): RespCommandArguments

  final def contramap[B](f: B => A): Input[B] =
    new Input[B] {
      def encode(data: B): RespCommandArguments = self.encode(f(data))
    }
}

object Input {
  def apply[A](implicit input: Input[A]): Input[A] = input

  case object AbsTtlInput extends Input[AbsTtl] {
    def encode(data: AbsTtl): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object AddressInput extends Input[Address] {
    def encode(data: Address): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.asString))
  }

  case object AggregateInput extends Input[Aggregate] {
    def encode(data: Aggregate): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("AGGREGATE"), RespCommandArgument.Literal(data.asString))
  }

  case object AlphaInput extends Input[Alpha] {
    def encode(data: Alpha): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  final case class ArbitraryKeyInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Key(data))
  }

  final case class ArbitraryValueInput[A: BinaryCodec]() extends Input[A] {
    def encode(data: A): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data))
  }

  case object AuthInput extends Input[Auth] {
    def encode(data: Auth): RespCommandArguments =
      data.username match {
        case Some(username) =>
          RespCommandArguments(RespCommandArgument.Value(username), RespCommandArgument.Value(data.password))
        case None => RespCommandArguments(RespCommandArgument.Value(data.password))
      }
  }

  case object BitFieldCommandInput extends Input[BitFieldCommand] {
    def encode(data: BitFieldCommand): RespCommandArguments = {
      import BitFieldCommand._

      val respArgs = data match {
        case BitFieldGet(t, o) =>
          Chunk(
            RespCommandArgument.Literal("GET"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString)
          )
        case BitFieldSet(t, o, v) =>
          Chunk(
            RespCommandArgument.Literal("SET"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString),
            RespCommandArgument.Value(v.toString)
          )
        case BitFieldIncr(t, o, i) =>
          Chunk(
            RespCommandArgument.Literal("INCRBY"),
            RespCommandArgument.Value(t.asString),
            RespCommandArgument.Value(o.toString),
            RespCommandArgument.Value(i.toString)
          )
        case bfo: BitFieldOverflow =>
          Chunk(RespCommandArgument.Literal("OVERFLOW"), RespCommandArgument.Literal(bfo.asString))
      }
      RespCommandArguments(respArgs)
    }
  }

  case object BitOperationInput extends Input[BitOperation] {
    def encode(data: BitOperation): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object BitPosRangeInput extends Input[BitPosRange] {
    def encode(data: BitPosRange): RespCommandArguments = {
      val start    = RespCommandArgument.Value(data.start.toString)
      val respArgs = data.end.fold(Chunk.single(start))(end => Chunk(start, RespCommandArgument.Value(end.toString)))
      RespCommandArguments(respArgs)
    }
  }

  case object BlockInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("BLOCK"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object BoolInput extends Input[Boolean] {
    def encode(data: Boolean): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(if (data) "1" else "0"))
  }

  case object ByInput extends Input[String] {
    def encode(data: String): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("BY"), RespCommandArgument.Value(data))
  }

  case object ChangedInput extends Input[Changed] {
    def encode(data: Changed): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object CopyInput extends Input[Copy] {
    def encode(data: Copy): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object CountInput extends Input[Count] {
    def encode(data: Count): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("COUNT"), RespCommandArgument.Value(data.count.toString))
  }

  case object DoubleInput extends Input[Double] {
    def encode(data: Double): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.toString))
  }

  case object DurationMillisecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.toMillis.toString))
  }

  case object DurationSecondsInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments = {
      val seconds = TimeUnit.MILLISECONDS.toSeconds(data.toMillis)
      RespCommandArguments(RespCommandArgument.Value(seconds.toString))
    }
  }

  case object DurationTtlInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments = {
      val milliseconds = data.toMillis
      RespCommandArguments(RespCommandArgument.Literal("PX"), RespCommandArgument.Value(milliseconds.toString))
    }
  }

  final case class EvalInput[-K, -V](inputK: Input[K], inputV: Input[V]) extends Input[(String, Chunk[K], Chunk[V])] {
    def encode(data: (String, Chunk[K], Chunk[V])): RespCommandArguments = {
      val (lua, keys, args) = data
      val encodedScript     = RespCommandArguments(RespCommandArgument.Value(lua), RespCommandArgument.Value(keys.size.toString))
      val encodedKeys = keys.foldLeft(RespCommandArguments.empty)((acc, a) =>
        acc ++ inputK.encode(a).map(arg => RespCommandArgument.Key(arg.value.value))
      )
      val encodedArgs = args.foldLeft(RespCommandArguments.empty)((acc, a) =>
        acc ++ inputV.encode(a).map(arg => RespCommandArgument.Value(arg.value.value))
      )
      encodedScript ++ encodedKeys ++ encodedArgs
    }
  }

  case object FreqInput extends Input[Freq] {
    def encode(data: Freq): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("FREQ"), RespCommandArgument.Value(data.frequency))
  }

  case object GetInput extends Input[String] {
    def encode(data: String): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("GET"), RespCommandArgument.Value(data))
  }

  final case class GetExInput[K: BinaryCodec]() extends Input[(K, Expire, Duration)] {
    def encode(data: (K, Expire, Duration)): RespCommandArguments =
      data match {
        case (key, Expire.SetExpireSeconds, duration) =>
          RespCommandArguments(RespCommandArgument.Key(key), RespCommandArgument.Literal("EX")) ++ DurationSecondsInput.encode(
            duration
          )
        case (key, Expire.SetExpireMilliseconds, duration) =>
          RespCommandArguments(RespCommandArgument.Key(key), RespCommandArgument.Literal("PX")) ++ DurationMillisecondsInput
            .encode(duration)
      }
  }

  final case class GetExAtInput[K: BinaryCodec]() extends Input[(K, ExpiredAt, Instant)] {
    def encode(data: (K, ExpiredAt, Instant)): RespCommandArguments =
      data match {
        case (key, ExpiredAt.SetExpireAtSeconds, instant) =>
          RespCommandArguments(RespCommandArgument.Key(key), RespCommandArgument.Literal("EXAT")) ++ TimeSecondsInput.encode(
            instant
          )
        case (key, ExpiredAt.SetExpireAtMilliseconds, instant) =>
          RespCommandArguments(RespCommandArgument.Key(key), RespCommandArgument.Literal("PXAT")) ++ TimeMillisecondsInput
            .encode(instant)
      }
  }

  final case class GetExPersistInput[K: BinaryCodec]() extends Input[(K, Boolean)] {
    def encode(data: (K, Boolean)): RespCommandArguments =
      RespCommandArguments(
        if (data._2) Chunk(RespCommandArgument.Key(data._1), RespCommandArgument.Literal("PERSIST"))
        else Chunk(RespCommandArgument.Key(data._1))
      )
  }

  case object GetKeywordInput extends Input[GetKeyword] {
    def encode(data: GetKeyword): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object IdleInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("IDLE"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object IdleTimeInput extends Input[IdleTime] {
    def encode(data: IdleTime): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("IDLETIME"), RespCommandArgument.Value(data.seconds.toString))
  }

  case object IdInput extends Input[Long] {
    def encode(data: Long): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("ID"), RespCommandArgument.Value(data.toString))
  }

  case object IdsInput extends Input[(Long, List[Long])] {
    def encode(data: (Long, List[Long])): RespCommandArguments =
      RespCommandArguments(
        Chunk.fromIterable(
          RespCommandArgument.Literal("ID") +: (data._1 :: data._2).map(id => RespCommandArgument.Value(id.toString))
        )
      )
  }

  case object IncrementInput extends Input[Increment] {
    def encode(data: Increment): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object IntInput extends Input[Int] {
    def encode(data: Int): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.toString))
  }

  case object KeepTtlInput extends Input[KeepTtl] {
    def encode(data: KeepTtl): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object LcsQueryTypeInput extends Input[LcsQueryType] {
    def encode(data: LcsQueryType): RespCommandArguments = data match {
      case LcsQueryType.Len => RespCommandArguments(RespCommandArgument.Literal("LEN"))
      case LcsQueryType.Idx(minMatchLength, withMatchLength) =>
        val idx = Chunk.single(RespCommandArgument.Literal("IDX"))
        val min =
          if (minMatchLength > 1)
            Chunk(RespCommandArgument.Literal("MINMATCHLEN"), RespCommandArgument.Value(minMatchLength.toString))
          else Chunk.empty[RespCommandArgument]
        val length =
          if (withMatchLength) Chunk.single(RespCommandArgument.Literal("WITHMATCHLEN"))
          else Chunk.empty[RespCommandArgument]
        RespCommandArguments(Chunk(idx, min, length).flatten)
    }
  }

  case object LimitInput extends Input[Limit] {
    def encode(data: Limit): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Literal("LIMIT"),
        RespCommandArgument.Value(data.offset.toString),
        RespCommandArgument.Value(data.count.toString)
      )
  }

  case object ListMaxLenInput extends Input[ListMaxLen] {
    def encode(data: ListMaxLen): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("MAXLEN"), RespCommandArgument.Value(data.count.toString))
  }

  case object LongInput extends Input[Long] {
    def encode(data: Long): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.toString))
  }

  case object LongLatInput extends Input[LongLat] {
    def encode(data: LongLat): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Value(data.longitude.toString),
        RespCommandArgument.Value(data.latitude.toString)
      )
  }

  final case class MemberScoreInput[M: BinaryCodec]() extends Input[MemberScore[M]] {
    def encode(data: MemberScore[M]): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.score.toString), RespCommandArgument.Value(data.member))
  }

  case object NoAckInput extends Input[NoAck] {
    def encode(data: NoAck): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.asString))
  }

  case object NoInput extends Input[Unit] {
    def encode(data: Unit): RespCommandArguments = RespCommandArguments.empty
  }

  final case class NonEmptyList[-A](input: Input[A]) extends Input[(A, List[A])] {
    def encode(data: (A, List[A])): RespCommandArguments =
      (data._1 :: data._2).foldLeft(RespCommandArguments.empty)((acc, a) => acc ++ input.encode(a))
  }

  final case class OptionalInput[-A](a: Input[A]) extends Input[Option[A]] {
    def encode(data: Option[A]): RespCommandArguments =
      data.fold(RespCommandArguments.empty)(a.encode)
  }

  case object OrderInput extends Input[Order] {
    def encode(data: Order): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.asString))
  }

  case object PatternInput extends Input[Pattern] {
    def encode(data: Pattern): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("MATCH"), RespCommandArgument.Value(data.pattern))
  }

  case object PositionInput extends Input[Position] {
    def encode(data: Position): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object RadiusUnitInput extends Input[RadiusUnit] {
    def encode(data: RadiusUnit): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.asString))
  }

  case object RangeInput extends Input[Range] {
    def encode(data: Range): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.start.toString), RespCommandArgument.Value(data.end.toString))
  }

  case object RankInput extends Input[Rank] {
    def encode(data: Rank): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("RANK"), RespCommandArgument.Value(data.rank.toString))
  }

  case object RedisTypeInput extends Input[RedisType] {
    def encode(data: RedisType): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("TYPE"), RespCommandArgument.Literal(data.asString))
  }

  case object ReplaceInput extends Input[Replace] {
    def encode(data: Replace): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object RetryCountInput extends Input[Long] {
    def encode(data: Long): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("RETRYCOUNT"), RespCommandArgument.Value(data.toString))
  }

  case object ScriptDebugInput extends Input[DebugMode] {
    def encode(data: DebugMode): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object ScriptFlushInput extends Input[FlushMode] {
    def encode(data: FlushMode): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object SideInput extends Input[Side] {
    def encode(data: Side): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object StoreDistInput extends Input[StoreDist] {
    def encode(data: StoreDist): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("STOREDIST"), RespCommandArgument.Value(data.key))
  }

  case object StoreInput extends Input[Store] {
    def encode(data: Store): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("STORE"), RespCommandArgument.Value(data.key))
  }

  case object StreamMaxLenInput extends Input[StreamMaxLen] {
    def encode(data: StreamMaxLen): RespCommandArguments = {
      val chunk =
        if (data.approximate) Chunk(RespCommandArgument.Literal("MAXLEN"), RespCommandArgument.Literal("~"))
        else Chunk.single(RespCommandArgument.Literal("MAXLEN"))

      RespCommandArguments(chunk :+ RespCommandArgument.Value(data.count.toString))
    }
  }

  final case class StreamsInput[K: BinaryCodec, V: BinaryCodec]() extends Input[((K, V), Chunk[(K, V)])] {
    def encode(data: ((K, V), Chunk[(K, V)])): RespCommandArguments = {
      val (keys, ids) = (data._1 +: data._2).map { case (key, value) =>
        (RespCommandArgument.Key(key), RespCommandArgument.Value(value))
      }.unzip

      RespCommandArguments(Chunk.single(RespCommandArgument.Literal("STREAMS")) ++ keys ++ ids)
    }
  }

  case object StringInput extends Input[String] {
    def encode(data: String): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data))
  }

  case object TimeInput extends Input[Duration] {
    def encode(data: Duration): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal("TIME"), RespCommandArgument.Value(data.toMillis.toString))
  }

  case object TimeMillisecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.toEpochMilli.toString))
  }

  case object TimeSecondsInput extends Input[Instant] {
    def encode(data: Instant): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.getEpochSecond.toString))
  }

  final case class Tuple2[-A, -B](_1: Input[A], _2: Input[B]) extends Input[(A, B)] {
    def encode(data: (A, B)): RespCommandArguments =
      _1.encode(data._1) ++ _2.encode(data._2)
  }

  final case class Tuple3[-A, -B, -C](_1: Input[A], _2: Input[B], _3: Input[C]) extends Input[(A, B, C)] {
    def encode(data: (A, B, C)): RespCommandArguments =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3)
  }

  final case class Tuple4[-A, -B, -C, -D](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D])
      extends Input[(A, B, C, D)] {
    def encode(data: (A, B, C, D)): RespCommandArguments =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4)
  }

  final case class Tuple5[-A, -B, -C, -D, -E](_1: Input[A], _2: Input[B], _3: Input[C], _4: Input[D], _5: Input[E])
      extends Input[(A, B, C, D, E)] {
    def encode(data: (A, B, C, D, E)): RespCommandArguments =
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
    def encode(data: (A, B, C, D, E, F)): RespCommandArguments =
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
    def encode(data: (A, B, C, D, E, F, G)): RespCommandArguments =
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
    def encode(data: (A, B, C, D, E, F, G, H, I)): RespCommandArguments =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J)): RespCommandArguments =
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
    def encode(data: (A, B, C, D, E, F, G, H, I, J, K)): RespCommandArguments =
      _1.encode(data._1) ++ _2.encode(data._2) ++ _3.encode(data._3) ++ _4.encode(data._4) ++
        _5.encode(data._5) ++ _6.encode(data._6) ++ _7.encode(data._7) ++ _8.encode(data._8) ++
        _9.encode(data._9) ++ _10.encode(data._10) ++ _11.encode(data._11)
  }

  case object UpdateInput extends Input[Update] {
    def encode(data: Update): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data.asString))
  }

  case object ValueInput extends Input[Chunk[Byte]] {
    def encode(data: Chunk[Byte]): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Value(data))
  }

  final case class Varargs[-A](input: Input[A]) extends Input[Iterable[A]] {
    def encode(data: Iterable[A]): RespCommandArguments =
      data.foldLeft(RespCommandArguments.empty)((acc, a) => acc ++ input.encode(a))
  }

  case object WeightsInput extends Input[::[Double]] {
    def encode(data: ::[Double]): RespCommandArguments = {
      val args = data.foldLeft(Chunk.single[RespCommandArgument](RespCommandArgument.Literal("WEIGHTS"))) { (acc, a) =>
        acc ++ Chunk.single(RespCommandArgument.Value(a.toString))
      }
      RespCommandArguments(args)
    }
  }

  case object WithCoordInput extends Input[WithCoord] {
    def encode(data: WithCoord): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithDistInput extends Input[WithDist] {
    def encode(data: WithDist): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithForceInput extends Input[WithForce] {
    def encode(data: WithForce): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithHashInput extends Input[WithHash] {
    def encode(data: WithHash): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithJustIdInput extends Input[WithJustId] {
    def encode(data: WithJustId): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithScoreInput extends Input[WithScore] {
    def encode(data: WithScore): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  case object WithScoresInput extends Input[WithScores] {
    def encode(data: WithScores): RespCommandArguments =
      RespCommandArguments(RespCommandArgument.Literal(data.asString))
  }

  final case class XGroupCreateConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.CreateConsumer[K, G, C]] {
    def encode(data: XGroupCommand.CreateConsumer[K, G, C]): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Literal("CREATECONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.consumer)
      )
  }

  final case class XGroupCreateInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.Create[K, G, I]] {
    def encode(data: XGroupCommand.Create[K, G, I]): RespCommandArguments = {
      val chunk = Chunk(
        RespCommandArgument.Literal("CREATE"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.id)
      )

      RespCommandArguments(if (data.mkStream) chunk :+ RespCommandArgument.Literal(MkStream.asString) else chunk)
    }
  }

  final case class XGroupDelConsumerInput[K: BinaryCodec, G: BinaryCodec, C: BinaryCodec]()
      extends Input[XGroupCommand.DelConsumer[K, G, C]] {
    def encode(data: XGroupCommand.DelConsumer[K, G, C]): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Literal("DELCONSUMER"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.consumer)
      )
  }

  final case class XGroupDestroyInput[K: BinaryCodec, G: BinaryCodec]() extends Input[XGroupCommand.Destroy[K, G]] {
    def encode(data: XGroupCommand.Destroy[K, G]): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Literal("DESTROY"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group)
      )
  }

  final case class XGroupSetIdInput[K: BinaryCodec, G: BinaryCodec, I: BinaryCodec]()
      extends Input[XGroupCommand.SetId[K, G, I]] {
    def encode(data: XGroupCommand.SetId[K, G, I]): RespCommandArguments =
      RespCommandArguments(
        RespCommandArgument.Literal("SETID"),
        RespCommandArgument.Key(data.key),
        RespCommandArgument.Value(data.group),
        RespCommandArgument.Value(data.id)
      )
  }
}
