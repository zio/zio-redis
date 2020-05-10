package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Lists {
  final val brPopLPush = Command("BRPOPLPUSH", Tuple3(StringInput, StringInput, DurationInput), ByteOutput)
  final val lIndex     = Command("LINDEX", Tuple2(StringInput, LongInput), ByteOutput)
  final val lLen       = Command("LLEN", StringInput, LongOutput)
  final val lPop       = Command("LPOP", StringInput, ByteOutput)
  final val lPush      = Command("LPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val lPushX     = Command("LPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val lRange     = Command("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)
  final val lRem       = Command("LREM", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val lSet       = Command("LSET", Tuple3(StringInput, LongInput, ByteInput), UnitOutput)
  final val lTrim      = Command("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val rPop       = Command("RPOP", StringInput, ByteOutput)
  final val rPopLPush  = Command("RPOPLPUSH", Tuple2(StringInput, StringInput), ByteOutput)
  final val rPush      = Command("RPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val rPushX     = Command("RPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
}
