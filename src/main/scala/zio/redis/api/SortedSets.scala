package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

trait SortedSets {
  final val zcard    = Command("ZCARD", StringInput, LongOutput)
  final val zcount   = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val zincrby  = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val zrank    = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  final val zrem     = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val zrevrank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)
  final val zscan = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  final val zscore = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)
}
