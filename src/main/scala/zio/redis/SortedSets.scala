package zio.redis

import Command.Input._
import Command.Output._

trait SortedSets {
  val zcard    = Command("ZCARD", StringInput, LongOutput)
  val zcount   = Command("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  val zincrby  = Command("ZINCRBY", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  val zrank    = Command("ZRANK", Tuple2(StringInput, ByteInput), ByteOutput)
  val zrem     = Command("ZREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val zrevrank = Command("ZREVRANK", Tuple2(StringInput, ByteInput), LongOutput)
  val zscan = Command(
    "ZSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  val zscore = Command("ZSCORE", Tuple2(StringInput, ByteInput), LongOutput)
}
