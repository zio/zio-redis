package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Strings {
  final val append   = Command("APPEND", Tuple2(StringInput, ByteInput), LongOutput)
  final val bitCount = Command("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)

  final val bitField = Command(
    "BITFIELD",
    Tuple5(
      StringInput,
      OptionalInput(NonEmptyList(BitFieldGetInput)),
      OptionalInput(NonEmptyList(BitFieldSetInput)),
      OptionalInput(NonEmptyList(BitFieldIncrInput)),
      OptionalInput(NonEmptyList(BitFieldOverflowInput))
    ),
    ChunkOutput
  )

  final val bitOp =
    Command("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val bitPos =
    Command("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)

  final val decr        = Command("DECR", StringInput, LongOutput)
  final val decrBy      = Command("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val get         = Command("GET", StringInput, ByteOutput)
  final val getBit      = Command("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  final val getRange    = Command("GETRANGE", Tuple2(StringInput, RangeInput), ByteOutput)
  final val getSet      = Command("GETSET", Tuple2(StringInput, ByteInput), ByteOutput)
  final val incr        = Command("INCR", StringInput, LongOutput)
  final val incrBy      = Command("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val incrByFloat = Command("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), ByteOutput)
  final val mGet        = Command("MGET", NonEmptyList(StringInput), ChunkOutput)
  final val mSet        = Command("MSET", NonEmptyList(Tuple2(StringInput, ByteInput)), UnitOutput)
  final val mSetNx      = Command("MSETNX", NonEmptyList(Tuple2(StringInput, ByteInput)), BoolOutput)
  final val pSetEx      = Command("PSETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)

  final val set = Command(
    "SET",
    Tuple5(
      StringInput,
      ByteInput,
      OptionalInput(DurationInput),
      OptionalInput(UpdateInput),
      OptionalInput(KeepTtlInput)
    ),
    OptionalOutput(UnitOutput)
  )

  final val setBit   = Command("SETBIT", Tuple3(StringInput, LongInput, ByteInput), ByteOutput)
  final val setEx    = Command("SETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
  final val setNx    = Command("SETNX", Tuple2(StringInput, ByteInput), BoolOutput)
  final val setRange = Command("SETRANGE", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val strLen   = Command("STRLEN", StringInput, LongOutput)
}
