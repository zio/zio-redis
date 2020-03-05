package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

trait Lists {
  final val brpoplpush = Command("BRPOPLPUSH", Tuple3(StringInput, StringInput, DurationInput), ByteOutput)
  final val lindex     = Command("LINDEX", Tuple2(StringInput, LongInput), ByteOutput)
  final val llen       = Command("LLEN", StringInput, LongOutput)
  final val lpop       = Command("LPOP", StringInput, ByteOutput)
  final val lpush      = Command("LPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val lpushx     = Command("LPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val lrange     = Command("LRANGE", Tuple2(StringInput, RangeInput), StreamOutput)
  final val lrem       = Command("LREM", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val lset       = Command("LSET", Tuple3(StringInput, LongInput, ByteInput), UnitOutput)
  final val ltrim      = Command("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val rpop       = Command("RPOP", StringInput, ByteOutput)
  final val rpoplpush  = Command("RPOPLPUSH", Tuple2(StringInput, StringInput), ByteOutput)
  final val rpush      = Command("RPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val rpushx     = Command("RPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
}
