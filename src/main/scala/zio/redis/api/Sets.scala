package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

trait Sets {
  val sadd        = Command("SADD", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val scard       = Command("SCARD", StringInput, LongOutput)
  val sdiff       = Command("SDIFF", NonEmptyList(StringInput), ByteOutput)
  val sdiffstore  = Command("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  val sinter      = Command("SINTER", NonEmptyList(StringInput), ByteOutput)
  val sinterstore = Command("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  val sismember   = Command("SISMEMBER", Tuple2(StringInput, ByteInput), BoolOutput)
  val smembers    = Command("SMEMBERS", StringInput, ByteOutput)
  val smove       = Command("SMOVE", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  val spop        = Command("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
  val srandmember = Command("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
  val srem        = Command("SREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val sscan = Command(
    "SSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  val sunion      = Command("SUNION", NonEmptyList(StringInput), ByteOutput)
  val sunionstore = Command("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
