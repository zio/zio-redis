package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Lists {
  final val brPopLPush =
    RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput),
      Base
    )

  final val lIndex    = RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput), Base)
  final val lLen      = RedisCommand("LLEN", StringInput, LongOutput, Base)
  final val lPop      = RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput), Base)
  final val lPush     = RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val lPushX    = RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val lRange    = RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput, Base)
  final val lRem      = RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput, Base)
  final val lSet      = RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput, Base)
  final val lTrim     = RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput, Base)
  final val rPop      = RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput), Base)
  final val rPopLPush =
    RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput), Base)
  final val rPush     = RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val rPushX    = RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val blPop     =
    RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput, Base)
  final val brPop     =
    RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput, Base)
  final val lInsert   =
    RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput, Base)
}
