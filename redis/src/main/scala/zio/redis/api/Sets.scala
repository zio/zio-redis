package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Sets {
  final val sAdd        = RedisCommand("SADD", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sCard       = RedisCommand("SCARD", StringInput, LongOutput)
  final val sDiff       = RedisCommand("SDIFF", NonEmptyList(StringInput), ChunkOutput)
  final val sDiffStore  = RedisCommand("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sInter      = RedisCommand("SINTER", NonEmptyList(StringInput), ChunkOutput)
  final val sInterStore = RedisCommand("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sIsMember   = RedisCommand("SISMEMBER", Tuple2(StringInput, StringInput), BoolOutput)
  final val sMembers    = RedisCommand("SMEMBERS", StringInput, ChunkOutput)
  final val sMove       = RedisCommand("SMOVE", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val sPop        =
    RedisCommand("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput)

  // TODO: can have 2 different outputs depending on whether or not count is provided
  final val sRandMember = RedisCommand("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput)

  final val sRem = RedisCommand("SREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val sScan = RedisCommand(
    "SSCAN",
    Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
    ScanOutput
  )

  final val sUnion      = RedisCommand("SUNION", NonEmptyList(StringInput), ChunkOutput)
  final val sUnionStore = RedisCommand("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
