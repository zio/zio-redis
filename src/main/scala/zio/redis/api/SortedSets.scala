package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait SortedSets {
  final val bzpopmin = Command("BZPOPMIN", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)
  final val bzpopmax = Command("BZPOPMAX", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)

  final val zadd = Command(
    "ZADD",
    Tuple5(
      StringInput,
      OptionalInput(UpdatesInput),
      OptionalInput(SortedSetChangedInput),
      OptionalInput(SortedSetIncrementInput),
      NonEmptyList(SortedSetMemberScoreInput)
    ),
    LongOutput
  )

  final val zcard   = Command("ZCARD", StringInput, LongOutput)
  final val zcount  = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zincrby = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)

  final val zlexcount = Command("ZLEXCOUNT", Tuple2(StringInput, SortedSetLexRangeInput), LongOutput)

  final val zinterstore = Command(
    "ZINTERSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(SortedSetAggregateInput)
    ),
    LongOutput
  )

  final val zpopmax = Command("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), StreamOutput)
  final val zpopmin = Command("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), StreamOutput)

  final val zrange =
    Command("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(SortedSetWithScoresInput)), StreamOutput)

  final val zrangebylex = Command(
    "ZRANGEBYLEX",
    Tuple3(
      StringInput,
      SortedSetLexRangeInput,
      OptionalInput(SortedSetLimitInput)
    ),
    StreamOutput
  )

  final val zrangebyscore = Command(
    "ZRANGEBYSCORE",
    Tuple4(
      StringInput,
      SortedSetScoreRangeInput,
      OptionalInput(SortedSetWithScoresInput),
      OptionalInput(SortedSetLimitInput)
    ),
    StreamOutput
  )

  final val zrank = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  final val zrem  = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)

  final val zremrangebylex   = Command("ZREMRANGEBYLEX", Tuple2(StringInput, SortedSetLexRangeInput), LongOutput)
  final val zremrangebyrank  = Command("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val zremrangebyscore = Command("ZREMRANGEBYSCORE", Tuple2(StringInput, SortedSetScoreRangeInput), LongOutput)

  final val zrevrangebylex = Command(
    "ZREVRANGEBYLEX",
    Tuple3(
      StringInput,
      SortedSetLexRangeInput,
      OptionalInput(SortedSetLimitInput)
    ),
    StreamOutput
  )
  final val zrevrangebyscore = Command(
    "ZREVRANGEBYSCORE",
    Tuple4(
      StringInput,
      SortedSetScoreRangeInput,
      OptionalInput(SortedSetWithScoresInput),
      OptionalInput(SortedSetLimitInput)
    ),
    StreamOutput
  )

  final val revrange =
    Command("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(SortedSetWithScoresInput)), StreamOutput)
  final val zrevrank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)
  final val zscan = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )
  final val zscore = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)

  final val zunionstore = Command(
    "ZUNIONSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(SortedSetAggregateInput)
    ),
    LongOutput
  )
}
