package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Hashes {
  final val hDel         = Command("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val hExists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  final val hGet         = Command("HGET", Tuple2(StringInput, StringInput), ByteOutput)
  final val hGetAll      = Command("HGETALL", StringInput, ChunkOutput)
  final val hIncrBy      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
  final val hIncrByFloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ByteOutput)
  final val hKeys        = Command("HKEYS", StringInput, ChunkOutput)
  final val hLen         = Command("HLEN", StringInput, LongOutput)
  final val hmGet        = Command("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val hScan = Command(
    "HSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val hSet = Command(
    "HSET",
    Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, ByteInput))),
    LongOutput
  )

  final val hSetNx  = Command("HSETNX", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  final val hStrLen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val hVals   = Command("HVALS", StringInput, ChunkOutput)
}
