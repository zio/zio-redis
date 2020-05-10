package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Sets {
  final val sAdd        = Command("SADD", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val sCard       = Command("SCARD", StringInput, LongOutput)
  final val sDiff       = Command("SDIFF", NonEmptyList(StringInput), ChunkOutput)
  final val sDiffStore  = Command("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sInter      = Command("SINTER", NonEmptyList(StringInput), ChunkOutput)
  final val sInterStore = Command("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sIsMember   = Command("SISMEMBER", Tuple2(StringInput, ByteInput), BoolOutput)
  final val sMembers    = Command("SMEMBERS", StringInput, ChunkOutput)
  final val sMove       = Command("SMOVE", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  final val sPop        = Command("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
  final val sRandMember = Command("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val sRem        = Command("SREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)

  final val sScan = Command(
    "SSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val sUnion      = Command("SUNION", NonEmptyList(StringInput), ChunkOutput)
  final val sUnionStore = Command("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
