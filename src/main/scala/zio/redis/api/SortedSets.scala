package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait SortedSets {
  final val bzpopmin = Command("BZPOPMIN", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)
  final val bzpopmax = Command("BZPOPMAX", Tuple2(DurationInput, NonEmptyList(StringInput)), ByteOutput)

  final val zadd = Command(
    "ZADD",
    Tuple6(
      StringInput,
      OptionalInput(ZAddUpdateInput),
      OptionalInput(ZAddChangedInput),
      OptionalInput(ZAddIncrementInput),
      LongInput,
      NonEmptyList(StringInput)
    ),
    LongOutput
  )

  final val zcard   = Command("ZCARD", StringInput, LongOutput)
  final val zcount  = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zincrby = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val zrange =
    Command("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(ZRangeWithScoresInput)), StreamOutput)
  final val zrank    = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  final val zrem     = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val zrevrank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)
  final val zscan = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )
  final val zscore = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)
}
