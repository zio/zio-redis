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

package zio.redis.commands

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema
import zio.{Chunk, Duration}

private[redis] trait SortedSets extends RedisEnvironment {
  import SortedSets._

  final def _bzPopMax[K: Schema, M: Schema]: RedisCommand[((K, List[K]), Duration), Option[(K, MemberScore[M])]] = {
    val memberScoreOutput =
      Tuple3Output(ArbitraryOutput[K](), ArbitraryOutput[M](), DoubleOutput).map { case (k, m, s) =>
        (k, MemberScore(s, m))
      }

    RedisCommand(
      BzPopMax,
      Tuple2(NonEmptyList(ArbitraryKeyInput[K]()), DurationSecondsInput),
      OptionalOutput(memberScoreOutput),
      codec,
      executor
    )
  }

  final def _bzPopMin[K: Schema, M: Schema]: RedisCommand[((K, List[K]), Duration), Option[(K, MemberScore[M])]] = {
    val memberScoreOutput =
      Tuple3Output(ArbitraryOutput[K](), ArbitraryOutput[M](), DoubleOutput).map { case (k, m, s) =>
        (k, MemberScore(s, m))
      }

    RedisCommand(
      BzPopMin,
      Tuple2(NonEmptyList(ArbitraryKeyInput[K]()), DurationSecondsInput),
      OptionalOutput(memberScoreOutput),
      codec,
      executor
    )
  }

  final def _zAdd[K: Schema, M: Schema]
    : RedisCommand[(K, Option[Update], Option[Changed], (MemberScore[M], List[MemberScore[M]])), Long] = RedisCommand(
    ZAdd,
    Tuple4(
      ArbitraryKeyInput[K](),
      OptionalInput(UpdateInput),
      OptionalInput(ChangedInput),
      NonEmptyList(MemberScoreInput[M]())
    ),
    LongOutput,
    codec,
    executor
  )

  final def _zAddWithIncr[K: Schema, M: Schema]: RedisCommand[
    (
      K,
      Option[zio.redis.Update],
      Option[Changed],
      Increment,
      (zio.redis.MemberScore[M], List[zio.redis.MemberScore[M]])
    ),
    Option[Double]
  ] = RedisCommand(
    ZAdd,
    Tuple5(
      ArbitraryKeyInput[K](),
      OptionalInput(UpdateInput),
      OptionalInput(ChangedInput),
      IncrementInput,
      NonEmptyList(MemberScoreInput[M]())
    ),
    OptionalOutput(DoubleOutput),
    codec,
    executor
  )

  final def _zCard[K: Schema]: RedisCommand[K, Long] =
    RedisCommand(ZCard, ArbitraryKeyInput[K](), LongOutput, codec, executor)

  final def _zCount[K: Schema]: RedisCommand[(K, Range), Long] =
    RedisCommand(ZCount, Tuple2(ArbitraryKeyInput[K](), RangeInput), LongOutput, codec, executor)

  final def _zDiff[K: Schema, M: Schema]: RedisCommand[(Long, (K, List[K])), Chunk[M]] = RedisCommand(
    ZDiff,
    Tuple2(
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]())
    ),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zDiffWithScores[K: Schema, M: Schema]: RedisCommand[(Long, (K, List[K]), String), Chunk[MemberScore[M]]] =
    RedisCommand(
      ZDiff,
      Tuple3(
        LongInput,
        NonEmptyList(ArbitraryKeyInput[K]()),
        ArbitraryValueInput[String]()
      ),
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )

  final def _zDiffStore[DK: Schema, K: Schema]: RedisCommand[(DK, Long, (K, List[K])), Long] = RedisCommand(
    ZDiffStore,
    Tuple3(
      ArbitraryValueInput[DK](),
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]())
    ),
    LongOutput,
    codec,
    executor
  )

  final def _zIncrBy[K: Schema, M: Schema]: RedisCommand[(K, Long, M), Double] =
    RedisCommand(
      ZIncrBy,
      Tuple3(ArbitraryKeyInput[K](), LongInput, ArbitraryValueInput[M]()),
      DoubleOutput,
      codec,
      executor
    )

  final def _zInter[K: Schema, M: Schema]
    : RedisCommand[(Long, (K, List[K]), Option[Aggregate], Option[::[Double]]), Chunk[M]] = RedisCommand(
    ZInter,
    Tuple4(
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]()),
      OptionalInput(AggregateInput),
      OptionalInput(WeightsInput)
    ),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zInterWithScores[K: Schema, M: Schema]
    : RedisCommand[(Long, (K, List[K]), Option[zio.redis.Aggregate], Option[::[Double]], String), Chunk[
      MemberScore[M]
    ]] =
    RedisCommand(
      ZInter,
      Tuple5(
        LongInput,
        NonEmptyList(ArbitraryKeyInput[K]()),
        OptionalInput(AggregateInput),
        OptionalInput(WeightsInput),
        ArbitraryValueInput[String]()
      ),
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )

  final def _zInterStore[DK: Schema, K: Schema]
    : RedisCommand[(DK, Long, (K, List[K]), Option[zio.redis.Aggregate], Option[::[Double]]), Long] = RedisCommand(
    ZInterStore,
    Tuple5(
      ArbitraryValueInput[DK](),
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]()),
      OptionalInput(AggregateInput),
      OptionalInput(WeightsInput)
    ),
    LongOutput,
    codec,
    executor
  )

  final def _zLexCount[K: Schema]: RedisCommand[(K, String, String), Long] = RedisCommand(
    ZLexCount,
    Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    LongOutput,
    codec,
    executor
  )

  final def _zPopMax[K: Schema, M: Schema]: RedisCommand[(K, Option[Long]), Chunk[MemberScore[M]]] = RedisCommand(
    ZPopMax,
    Tuple2(ArbitraryKeyInput[K](), OptionalInput(LongInput)),
    ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
      .map(_.map { case (m, s) => MemberScore(s, m) }),
    codec,
    executor
  )

  final def _zPopMin[K: Schema, M: Schema]: RedisCommand[(K, Option[Long]), Chunk[MemberScore[M]]] = RedisCommand(
    ZPopMin,
    Tuple2(ArbitraryKeyInput[K](), OptionalInput(LongInput)),
    ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
      .map(_.map { case (m, s) => MemberScore(s, m) }),
    codec,
    executor
  )

  final def _zRange[K: Schema, M: Schema]: RedisCommand[(K, Range), Chunk[M]] = RedisCommand(
    ZRange,
    Tuple2(ArbitraryKeyInput[K](), RangeInput),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zRangeWithScores[K: Schema, M: Schema]: RedisCommand[(K, Range, String), Chunk[MemberScore[M]]] =
    RedisCommand(
      ZRange,
      Tuple3(ArbitraryKeyInput[K](), RangeInput, ArbitraryValueInput[String]()),
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )

  final def _zRangeByLex[K: Schema, M: Schema]: RedisCommand[(K, String, String, Option[Limit]), Chunk[M]] =
    RedisCommand(
      ZRangeByLex,
      Tuple4(
        ArbitraryKeyInput[K](),
        ArbitraryValueInput[String](),
        ArbitraryValueInput[String](),
        OptionalInput(LimitInput)
      ),
      ChunkOutput(ArbitraryOutput[M]()),
      codec,
      executor
    )

  final def _zRangeByScore[K: Schema, M: Schema]: RedisCommand[(K, String, String, Option[zio.redis.Limit]), Chunk[M]] =
    RedisCommand(
      ZRangeByScore,
      Tuple4(
        ArbitraryKeyInput[K](),
        ArbitraryValueInput[String](),
        ArbitraryValueInput[String](),
        OptionalInput(LimitInput)
      ),
      ChunkOutput(ArbitraryOutput[M]()),
      codec,
      executor
    )

  final def _zRangeByScoreWithScores[K: Schema, M: Schema]
    : RedisCommand[(K, String, String, String, Option[zio.redis.Limit]), Chunk[MemberScore[M]]] = RedisCommand(
    ZRangeByScore,
    Tuple5(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      OptionalInput(LimitInput)
    ),
    ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
      .map(_.map { case (m, s) => MemberScore(s, m) }),
    codec,
    executor
  )

  final def _zRank[K: Schema, M: Schema]: RedisCommand[(K, M), Option[Long]] =
    RedisCommand(
      ZRank,
      Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[M]()),
      OptionalOutput(LongOutput),
      codec,
      executor
    )

  final def _zRem[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Long] =
    RedisCommand(
      ZRem,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[M]())),
      LongOutput,
      codec,
      executor
    )

  final def _zRemRangeByLex[K: Schema]: RedisCommand[(K, String, String), Long] = RedisCommand(
    ZRemRangeByLex,
    Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    LongOutput,
    codec,
    executor
  )

  final def _zRemRangeByRank[K: Schema]: RedisCommand[(K, Range), Long] =
    RedisCommand(ZRemRangeByRank, Tuple2(ArbitraryKeyInput[K](), RangeInput), LongOutput, codec, executor)

  final def _zRemRangeByScore[K: Schema]: RedisCommand[(K, String, String), Long] = RedisCommand(
    ZRemRangeByScore,
    Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    LongOutput,
    codec,
    executor
  )

  final def _zRevRange[K: Schema, M: Schema]: RedisCommand[(K, Range), Chunk[M]] = RedisCommand(
    ZRevRange,
    Tuple2(ArbitraryKeyInput[K](), RangeInput),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zRevRangeWithScores[K: Schema, M: Schema]: RedisCommand[(K, Range, String), Chunk[MemberScore[M]]] =
    RedisCommand(
      ZRevRange,
      Tuple3(ArbitraryKeyInput[K](), RangeInput, ArbitraryValueInput[String]()),
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )

  final def _zRevRangeByLex[K: Schema, M: Schema]
    : RedisCommand[(K, String, String, Option[zio.redis.Limit]), Chunk[M]] = RedisCommand(
    ZRevRangeByLex,
    Tuple4(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      OptionalInput(LimitInput)
    ),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zRevRangeByScore[K: Schema, M: Schema]
    : RedisCommand[(K, String, String, Option[zio.redis.Limit]), Chunk[M]] = RedisCommand(
    ZRevRangeByScore,
    Tuple4(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      OptionalInput(LimitInput)
    ),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zRevRangeByScoreWithScores[K: Schema, M: Schema]
    : RedisCommand[(K, String, String, String, Option[zio.redis.Limit]), Chunk[MemberScore[M]]] = RedisCommand(
    ZRevRangeByScore,
    Tuple5(
      ArbitraryKeyInput[K](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      ArbitraryValueInput[String](),
      OptionalInput(LimitInput)
    ),
    ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
      .map(_.map { case (m, s) => MemberScore(s, m) }),
    codec,
    executor
  )

  final def _zRevRank[K: Schema, M: Schema]: RedisCommand[(K, M), Option[Long]] = RedisCommand(
    ZRevRank,
    Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[M]()),
    OptionalOutput(LongOutput),
    codec,
    executor
  )

  final def _zScan[K: Schema, M: Schema]
    : RedisCommand[(K, Long, Option[Pattern], Option[Count]), (Long, Chunk[MemberScore[M]])] = {
    val memberScoresOutput =
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput).map(_.map { case (m, s) => MemberScore(s, m) })

    RedisCommand(
      ZScan,
      Tuple4(ArbitraryKeyInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
      Tuple2Output(MultiStringOutput.map(_.toLong), memberScoresOutput),
      codec,
      executor
    )
  }

  final def _zScore[K: Schema, M: Schema]: RedisCommand[(K, M), Option[Double]] = RedisCommand(
    ZScore,
    Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[M]()),
    OptionalOutput(DoubleOutput),
    codec,
    executor
  )

  final def _zUnion[K: Schema, M: Schema]
    : RedisCommand[(Long, (K, List[K]), Option[::[Double]], Option[zio.redis.Aggregate]), Chunk[M]] = RedisCommand(
    ZUnion,
    Tuple4(
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]()),
      OptionalInput(WeightsInput),
      OptionalInput(AggregateInput)
    ),
    ChunkOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zUnionWithScores[K: Schema, M: Schema]: RedisCommand[
    (Long, (K, List[K]), Option[::[Double]], Option[zio.redis.Aggregate], String),
    Chunk[MemberScore[M]]
  ] =
    RedisCommand(
      ZUnion,
      Tuple5(
        LongInput,
        NonEmptyList(ArbitraryKeyInput[K]()),
        OptionalInput(WeightsInput),
        OptionalInput(AggregateInput),
        ArbitraryValueInput[String]()
      ),
      ChunkTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )

  final def _zUnionStore[DK: Schema, K: Schema]: RedisCommand[
    (DK, Long, (K, List[K]), Option[::[Double]], Option[zio.redis.Aggregate]),
    Long
  ] = RedisCommand(
    ZUnionStore,
    Tuple5(
      ArbitraryValueInput[DK](),
      LongInput,
      NonEmptyList(ArbitraryKeyInput[K]()),
      OptionalInput(WeightsInput),
      OptionalInput(AggregateInput)
    ),
    LongOutput,
    codec,
    executor
  )

  final def _zMScore[K: Schema]: RedisCommand[(K, List[K]), Chunk[Option[Double]]] =
    RedisCommand(
      ZMScore,
      NonEmptyList(ArbitraryKeyInput[K]()),
      ChunkOutput(OptionalOutput(DoubleOutput)),
      codec,
      executor
    )

  final def _zRandMember[K: Schema, R: Schema]: RedisCommand[K, Option[R]] =
    RedisCommand(ZRandMember, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[R]()), codec, executor)

  final def _zRandMemberWithCount[K: Schema, M: Schema]: RedisCommand[(K, Long), Chunk[M]] = RedisCommand(
    ZRandMember,
    Tuple2(ArbitraryKeyInput[K](), LongInput),
    ZRandMemberOutput(ArbitraryOutput[M]()),
    codec,
    executor
  )

  final def _zRandMemberWithScores[K: Schema, M: Schema]: RedisCommand[(K, Long, String), Chunk[MemberScore[M]]] =
    RedisCommand(
      ZRandMember,
      Tuple3(ArbitraryKeyInput[K](), LongInput, ArbitraryValueInput[String]()),
      ZRandMemberTuple2Output(ArbitraryOutput[M](), DoubleOutput)
        .map(_.map { case (m, s) => MemberScore(s, m) }),
      codec,
      executor
    )
}

private object SortedSets {
  final val BzPopMax         = "BZPOPMAX"
  final val BzPopMin         = "BZPOPMIN"
  final val ZAdd             = "ZADD"
  final val ZCard            = "ZCARD"
  final val ZCount           = "ZCOUNT"
  final val ZDiff            = "ZDIFF"
  final val ZDiffStore       = "ZDIFFSTORE"
  final val ZIncrBy          = "ZINCRBY"
  final val ZInter           = "ZINTER"
  final val ZInterStore      = "ZINTERSTORE"
  final val ZLexCount        = "ZLEXCOUNT"
  final val ZMScore          = "ZMSCORE"
  final val ZPopMax          = "ZPOPMAX"
  final val ZPopMin          = "ZPOPMIN"
  final val ZRange           = "ZRANGE"
  final val ZRangeByLex      = "ZRANGEBYLEX"
  final val ZRangeByScore    = "ZRANGEBYSCORE"
  final val ZRank            = "ZRANK"
  final val ZRem             = "ZREM"
  final val ZRemRangeByLex   = "ZREMRANGEBYLEX"
  final val ZRemRangeByRank  = "ZREMRANGEBYRANK"
  final val ZRemRangeByScore = "ZREMRANGEBYSCORE"
  final val ZRevRange        = "ZREVRANGE"
  final val ZRevRangeByLex   = "ZREVRANGEBYLEX"
  final val ZRevRangeByScore = "ZREVRANGEBYSCORE"
  final val ZRevRank         = "ZREVRANK"
  final val ZScan            = "ZSCAN"
  final val ZScore           = "ZSCORE"
  final val ZUnion           = "ZUNION"
  final val ZUnionStore      = "ZUNIONSTORE"
  final val ZRandMember      = "ZRANDMEMBER"
}
