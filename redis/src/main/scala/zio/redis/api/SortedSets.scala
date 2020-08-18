package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait SortedSets {
  final val bzPopMax =
    RedisCommand("BZPOPMAX", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput, Base)
  final val bzPopMin =
    RedisCommand("BZPOPMIN", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput, Base)

  final def zAdd =
    RedisCommand(
      "ZADD",
      Tuple4(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        NonEmptyList(MemberScoreInput)
      ),
      LongOutput,
      Base
    )

  final def zAddWithIncr =
    RedisCommand(
      "ZADD",
      Tuple5(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        IncrementInput,
        NonEmptyList(MemberScoreInput)
      ),
      OptionalOutput(DoubleOutput),
      Base
    )

  final val zCard   = RedisCommand("ZCARD", StringInput, LongOutput, Base)
  final val zCount  = RedisCommand("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput, Base)
  final val zIncrBy = RedisCommand("ZINCRBY", Tuple3(StringInput, LongInput, StringInput), DoubleOutput, Base)

  final val zInterStore = RedisCommand(
    "ZINTERSTORE",
    Tuple5(
      StringInput,
      LongInput,
      NonEmptyList(StringInput),
      OptionalInput(AggregateInput),
      OptionalInput(WeightsInput)
    ),
    LongOutput,
    Base
  )

  final val zLexCount = RedisCommand("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput, Base)
  final val zPopMax   = RedisCommand("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput, Base)
  final val zPopMin   = RedisCommand("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput, Base)

  final val zRange =
    RedisCommand("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput, Base)

  final val zRangeByLex =
    RedisCommand("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput, Base)

  final val zRangeByScore = RedisCommand(
    "ZRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput,
    Base
  )

  final val zRank            = RedisCommand("ZRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput), Base)
  final val zRem             = RedisCommand("ZREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val zRemRangeByLex   = RedisCommand("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput, Base)
  final val zRemRangeByRank  = RedisCommand("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput, Base)
  final val zRemRangeByScore = RedisCommand("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput, Base)

  final val zRevRange =
    RedisCommand("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput, Base)

  final val zRevRangeByLex =
    RedisCommand("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput, Base)

  final val zRevRangeByScore = RedisCommand(
    "ZREVRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput,
    Base
  )

  final val zRevRank = RedisCommand("ZREVRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput), Base)

  final val zScan = RedisCommand(
    "ZSCAN",
    Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
    ScanOutput,
    Base
  )

  final val zScore = RedisCommand("ZSCORE", Tuple2(StringInput, StringInput), OptionalOutput(DoubleOutput), Base)

  final val zUnionStore = RedisCommand(
    "ZUNIONSTORE",
    Tuple5(
      StringInput,
      LongInput,
      NonEmptyList(StringInput),
      OptionalInput(WeightsInput),
      OptionalInput(AggregateInput)
    ),
    LongOutput,
    Base
  )
}
