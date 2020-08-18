package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Hashes {
  final val hDel         = RedisCommand("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val hExists      = RedisCommand("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput, Base)
  final val hGet         = RedisCommand("HGET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput), Base)
  final val hGetAll      = RedisCommand("HGETALL", StringInput, KeyValueOutput, Base)
  final val hIncrBy      = RedisCommand("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput, Base)
  final val hIncrByFloat =
    RedisCommand("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), IncrementOutput, Base)
  final val hKeys        = RedisCommand("HKEYS", StringInput, ChunkOutput, Base)
  final val hLen         = RedisCommand("HLEN", StringInput, LongOutput, Base)
  final val hmGet        = RedisCommand("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput, Base)

  final val hScan = RedisCommand(
    "HSCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput,
    Base
  )

  final val hSet = RedisCommand(
    "HSET",
    Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
    LongOutput,
    Base
  )

  final val hSetNx  = RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput, Base)
  final val hStrLen = RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput, Base)
  final val hVals   = RedisCommand("HVALS", StringInput, ChunkOutput, Base)
}
