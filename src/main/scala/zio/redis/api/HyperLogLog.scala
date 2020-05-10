package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait HyperLogLog {
  final val pfAdd   = Command("PFADD", Tuple2(StringInput, NonEmptyList(ByteInput)), BoolOutput)
  final val pfCount = Command("PFCOUNT", NonEmptyList(StringInput), LongOutput)
  final val pfMerge = Command("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
}
