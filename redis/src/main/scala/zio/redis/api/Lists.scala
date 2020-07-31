package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Lists {
  final val brPopLPush =
    RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val lIndex    = RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput))
  final val lLen      = RedisCommand("LLEN", StringInput, LongOutput)
  final val lPop      = RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput))
  final val lPush     = RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val lPushX    = RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val lRange    = RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)
  final val lRem      = RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val lSet      = RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput)
  final val lTrim     = RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val rPop      = RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput))
  final val rPopLPush = RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val rPush     = RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val rPushX    = RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val blPop     =
    RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), TimeSecondsInput), OptionalOutput(BLPopOutput))
}
