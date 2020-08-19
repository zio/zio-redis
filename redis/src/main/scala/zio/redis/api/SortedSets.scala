package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait SortedSets {
  final val bzPopMax = RedisCommand("BZPOPMAX", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)
  final val bzPopMin = RedisCommand("BZPOPMIN", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)

  // TODO: represent two possible outputs as per the documentation
  final val zAdd = RedisCommand(
    "ZADD",
    Tuple5(
      StringInput,
      OptionalInput(UpdateInput),
      OptionalInput(ChangedInput),
      OptionalInput(IncrementInput),
      NonEmptyList(MemberScoreInput)
    ),
    LongOutput
  )

  final val zCard   = RedisCommand("ZCARD", StringInput, LongOutput)
  final val zCount  = RedisCommand("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zIncrBy = RedisCommand("ZINCRBY", Tuple3(StringInput, LongInput, StringInput), DoubleOutput)

  final val zInterStore = RedisCommand(
    "ZINTERSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(AggregateInput)
    ),
    LongOutput
  )

  final val zLexCount = RedisCommand("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val zPopMax   = RedisCommand("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val zPopMin   = RedisCommand("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val zRange =
    RedisCommand("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val zRangeByLex =
    RedisCommand("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val zRangeByScore = RedisCommand(
    "ZRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput
  )

  final val zRank            = RedisCommand("ZRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))
  final val zRem             = RedisCommand("ZREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val zRemRangeByLex   = RedisCommand("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val zRemRangeByRank  = RedisCommand("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val zRemRangeByScore = RedisCommand("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput)

  final val zRevRange =
    RedisCommand("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val zRevRangeByLex =
    RedisCommand("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val zRevRangeByScore = RedisCommand(
    "ZREVRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput
  )

  final val zRevRank = RedisCommand("ZREVRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))

  final val zScan = RedisCommand(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val zScore = RedisCommand("ZSCORE", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))

  final val zUnionStore = RedisCommand(
    "ZUNIONSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(AggregateInput)
    ),
    LongOutput
  )
}
