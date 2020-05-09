package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Sets {
  final val sadd        = Command("SADD", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val scard       = Command("SCARD", StringInput, LongOutput)
  final val sdiff       = Command("SDIFF", NonEmptyList(StringInput), ByteOutput)
  final val sdiffstore  = Command("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sinter      = Command("SINTER", NonEmptyList(StringInput), ByteOutput)
  final val sinterstore = Command("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val sismember   = Command("SISMEMBER", Tuple2(StringInput, ByteInput), BoolOutput)
  final val smembers    = Command("SMEMBERS", StringInput, ByteOutput)
  final val smove       = Command("SMOVE", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  final val spop        = Command("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
  final val srandmember = Command("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
  final val srem        = Command("SREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  final val sscan       = Command(
    "SSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )
  final val sunion      = Command("SUNION", NonEmptyList(StringInput), ByteOutput)
  final val sunionstore = Command("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
