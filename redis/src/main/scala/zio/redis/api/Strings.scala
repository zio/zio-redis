package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Strings {
  final val append   = RedisCommand("APPEND", Tuple2(StringInput, StringInput), LongOutput)
  final val bitCount = RedisCommand("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)

  final val bitField = RedisCommand(
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
    RedisCommand("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val bitPos =
    RedisCommand("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)

  final val decr        = RedisCommand("DECR", StringInput, LongOutput)
  final val decrBy      = RedisCommand("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val get         = RedisCommand("GET", StringInput, OptionalOutput(StringOutput))
  final val getBit      = RedisCommand("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  final val getRange    = RedisCommand("GETRANGE", Tuple2(StringInput, RangeInput), StringOutput)
  final val getSet      = RedisCommand("GETSET", Tuple2(StringInput, StringInput), OptionalOutput(StringOutput))
  final val incr        = RedisCommand("INCR", StringInput, LongOutput)
  final val incrBy      = RedisCommand("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val incrByFloat = RedisCommand("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), StringOutput)
  final val mGet        = RedisCommand("MGET", NonEmptyList(StringInput), ChunkOutput)
  final val mSet        = RedisCommand("MSET", NonEmptyList(Tuple2(StringInput, StringInput)), UnitOutput)
  final val mSetNx      = RedisCommand("MSETNX", NonEmptyList(Tuple2(StringInput, StringInput)), BoolOutput)
  final val pSetEx      = RedisCommand("PSETEX", Tuple3(StringInput, DurationMillisecondsInput, StringInput), UnitOutput)

  final val set = RedisCommand(
    "SET",
    Tuple5(
      StringInput,
      StringInput,
      OptionalInput(DurationSecondsInput),
      OptionalInput(UpdateInput),
      OptionalInput(KeepTtlInput)
    ),
    OptionalOutput(UnitOutput)
  )

  final val setBit   = RedisCommand("SETBIT", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val setEx    = RedisCommand("SETEX", Tuple3(StringInput, DurationSecondsInput, StringInput), UnitOutput)
  final val setNx    = RedisCommand("SETNX", Tuple2(StringInput, StringInput), BoolOutput)
  final val setRange = RedisCommand("SETRANGE", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val strLen   = RedisCommand("STRLEN", StringInput, LongOutput)
}
