package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait HyperLogLog {
  final val pfadd   = Command("PFADD", Tuple2(StringInput, NonEmptyList(ByteInput)), BoolOutput)
  final val pfcount = Command("PFCOUNT", NonEmptyList(StringInput), LongOutput)
  final val pfmerge = Command("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
}
