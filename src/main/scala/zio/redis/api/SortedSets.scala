package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait SortedSets {
  final val bzPopMin = Command("BZPOPMIN", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)
  final val bzPopMax = Command("BZPOPMAX", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)

  final val zAdd = Command(
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

  final val zCard     = Command("ZCARD", StringInput, LongOutput)
  final val zCount    = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zIncrBy   = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val zLexCount = Command("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput)

  final val zInterStore = Command(
    "ZINTERSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(AggregateInput)
    ),
    LongOutput
  )

  final val zPopMax = Command("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val zPopMin = Command("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val zRange =
    Command("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val zRangeByLex =
    Command("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val zRangeByScore = Command(
    "ZRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput
  )

  final val zRank            = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  final val zRem             = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val zRemRangeByLex   = Command("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val zRemRangeByRank  = Command("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val zRemRangeByScore = Command("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput)

  final val zRevRangeByLex =
    Command("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val zRevRangeByScore = Command(
    "ZREVRANGEBYSCORE",
    Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
    ChunkOutput
  )

  final val revRange =
    Command("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val zRevRank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)

  final val zScan = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val zScore = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)

  final val zUnionStore = Command(
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
