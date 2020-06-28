package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Hashes {
  final val hDel         = RedisCommand("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val hExists      = RedisCommand("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  final val hGet         = RedisCommand("HGET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val hGetAll      = RedisCommand("HGETALL", StringInput, ChunkOutput)
  final val hIncrBy      = RedisCommand("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
  final val hIncrByFloat =
    RedisCommand("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), MultiStringOutput)
  final val hKeys        = RedisCommand("HKEYS", StringInput, ChunkOutput)
  final val hLen         = RedisCommand("HLEN", StringInput, LongOutput)
  final val hmGet        = RedisCommand("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val hScan = RedisCommand(
    "HSCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val hSet = RedisCommand(
    "HSET",
    Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
    LongOutput
  )

  final val hSetNx  = RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val hStrLen = RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val hVals   = RedisCommand("HVALS", StringInput, ChunkOutput)
}
