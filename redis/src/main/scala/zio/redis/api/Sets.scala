package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Sets {
  final val sAdd        = RedisCommand("SADD", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val sCard       = RedisCommand("SCARD", StringInput, LongOutput, Base)
  final val sDiff       = RedisCommand("SDIFF", NonEmptyList(StringInput), ChunkOutput, Base)
  final val sDiffStore  = RedisCommand("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val sInter      = RedisCommand("SINTER", NonEmptyList(StringInput), ChunkOutput, Base)
  final val sInterStore = RedisCommand("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
  final val sIsMember   = RedisCommand("SISMEMBER", Tuple2(StringInput, StringInput), BoolOutput, Base)
  final val sMembers    = RedisCommand("SMEMBERS", StringInput, ChunkOutput, Base)
  final val sMove       = RedisCommand("SMOVE", Tuple3(StringInput, StringInput, StringInput), BoolOutput, Base)
  final val sPop        =
    RedisCommand("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput, Base)

  final val sRandMember =
    RedisCommand("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput, Base)

  final val sRem = RedisCommand("SREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)

  final val sScan = RedisCommand(
    "SSCAN",
    Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
    ScanOutput,
    Base
  )

  final val sUnion      = RedisCommand("SUNION", NonEmptyList(StringInput), ChunkOutput, Base)
  final val sUnionStore = RedisCommand("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput, Base)
}
