package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Strings {
  final val append   = RedisCommand("APPEND", Tuple2(StringInput, StringInput), LongOutput, Base)
  final val bitCount = RedisCommand("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput, Base)

  final val bitField = RedisCommand(
    "BITFIELD",
    Tuple2(
      StringInput,
      NonEmptyList(BitFieldCommandInput)
    ),
    ChunkOptionalLongOutput,
    Base
  )

  final val bitOp =
    RedisCommand("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput, Base)

  final val bitPos =
    RedisCommand("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput, Base)

  final val decr        = RedisCommand("DECR", StringInput, LongOutput, Base)
  final val decrBy      = RedisCommand("DECRBY", Tuple2(StringInput, LongInput), LongOutput, Base)
  final val get         = RedisCommand("GET", StringInput, OptionalOutput(MultiStringOutput), Base)
  final val getBit      = RedisCommand("GETBIT", Tuple2(StringInput, LongInput), LongOutput, Base)
  final val getRange    = RedisCommand("GETRANGE", Tuple2(StringInput, RangeInput), MultiStringOutput, Base)
  final val getSet      = RedisCommand("GETSET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput), Base)
  final val incr        = RedisCommand("INCR", StringInput, LongOutput, Base)
  final val incrBy      = RedisCommand("INCRBY", Tuple2(StringInput, LongInput), LongOutput, Base)
  final val incrByFloat = RedisCommand("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), MultiStringOutput, Base)
  final val mGet        = RedisCommand("MGET", NonEmptyList(StringInput), ChunkOptionalMultiStringOutput, Base)
  final val mSet        = RedisCommand("MSET", NonEmptyList(Tuple2(StringInput, StringInput)), UnitOutput, Base)
  final val mSetNx      = RedisCommand("MSETNX", NonEmptyList(Tuple2(StringInput, StringInput)), BoolOutput, Base)
  final val pSetEx      =
    RedisCommand("PSETEX", Tuple3(StringInput, DurationMillisecondsInput, StringInput), UnitOutput, Base)

  final val set = RedisCommand(
    "SET",
    Tuple5(
      StringInput,
      StringInput,
      OptionalInput(DurationTtlInput),
      OptionalInput(UpdateInput),
      OptionalInput(KeepTtlInput)
    ),
    OptionalOutput(UnitOutput),
    Base
  )

  final val setBit   = RedisCommand("SETBIT", Tuple3(StringInput, LongInput, BoolInput), BoolOutput, Base)
  final val setEx    = RedisCommand("SETEX", Tuple3(StringInput, DurationSecondsInput, StringInput), UnitOutput, Base)
  final val setNx    = RedisCommand("SETNX", Tuple2(StringInput, StringInput), BoolOutput, Base)
  final val setRange = RedisCommand("SETRANGE", Tuple3(StringInput, LongInput, StringInput), LongOutput, Base)
  final val strLen   = RedisCommand("STRLEN", StringInput, LongOutput, Base)
}
