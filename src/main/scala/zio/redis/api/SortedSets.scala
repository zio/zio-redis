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
      OptionalInput(UpdateInput),
      OptionalInput(ChangedInput),
      OptionalInput(IncrementInput),
      NonEmptyList(MemberScoreInput)
    ),
    LongOutput
  )

  final val zcard   = Command("ZCARD", StringInput, LongOutput)
  final val zcount  = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zincrby = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)

  final val zlexcount = Command("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput)

  final val zinterstore = Command(
    "ZINTERSTORE",
    Tuple4(
      StringInput,
      NonEmptyList(StringInput),
      OptionalInput(NonEmptyList(DoubleInput)),
      OptionalInput(AggregateInput)
    ),
    LongOutput
  )

  final val zpopmax = Command("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val zpopmin = Command("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val zrange =
    Command("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val zrangebylex = Command(
    "ZRANGEBYLEX",
    Tuple3(
      StringInput,
      LexRangeInput,
      OptionalInput(LimitInput)
    ),
    ChunkOutput
  )

  final val zrangebyscore = Command(
    "ZRANGEBYSCORE",
    Tuple4(
      StringInput,
      ScoreRangeInput,
      OptionalInput(WithScoresInput),
      OptionalInput(LimitInput)
    ),
    ChunkOutput
  )

  final val zrank = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  final val zrem  = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)

  final val zremrangebylex   = Command("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val zremrangebyrank  = Command("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val zremrangebyscore = Command("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput)

  final val zrevrangebylex   = Command(
    "ZREVRANGEBYLEX",
    Tuple3(
      StringInput,
      LexRangeInput,
      OptionalInput(LimitInput)
    ),
    ChunkOutput
  )
  final val zrevrangebyscore = Command(
    "ZREVRANGEBYSCORE",
    Tuple4(
      StringInput,
      ScoreRangeInput,
      OptionalInput(WithScoresInput),
      OptionalInput(LimitInput)
    ),
    ChunkOutput
  )

  final val revrange =
    Command("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)
  final val zrevrank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)
  final val zscan    = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )
  final val zscore   = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)

  final val zunionstore = Command(
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
