package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Strings {
  final val append   = Command("APPEND", Tuple2(StringInput, ByteInput), LongOutput)
  final val bitcount = Command("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)

  final val bitfield = Command(
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

  final val bitop =
    Command(
      "BITOP",
      Tuple3(
        BitOperationInput,
        StringInput,
        NonEmptyList(StringInput)
      ),
      LongOutput
    )

  final val bitpos =
    Command("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)

  final val decr        = Command("DECR", StringInput, LongOutput)
  final val decrby      = Command("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val get         = Command("GET", StringInput, ByteOutput)
  final val getbit      = Command("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  final val getrange    = Command("GETRANGE", Tuple2(StringInput, RangeInput), ByteOutput)
  final val getset      = Command("GETSET", Tuple2(StringInput, ByteInput), ByteOutput)
  final val incr        = Command("INCR", StringInput, LongOutput)
  final val incrby      = Command("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val incrbyfloat = Command("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), ByteOutput)
  final val mget        = Command("MGET", NonEmptyList(StringInput), ChunkOutput)
  final val mset        = Command("MSET", NonEmptyList(Tuple2(StringInput, ByteInput)), UnitOutput)
  final val msetnx      = Command("MSETNX", NonEmptyList(Tuple2(StringInput, ByteInput)), BoolOutput)
  final val psetex      = Command("PSETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)

  final val set = Command(
    "SET",
    Tuple5(
      StringInput,
      ByteInput,
      OptionalInput(DurationInput),
      OptionalInput(UpdatesInput),
      OptionalInput(KeepTtlInput)
    ),
    OptionalOutput(UnitOutput)
  )

  final val setbit   = Command("SETBIT", Tuple3(StringInput, LongInput, ByteInput), ByteOutput)
  final val setex    = Command("SETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
  final val setnx    = Command("SETNX", Tuple2(StringInput, ByteInput), BoolOutput)
  final val setrange = Command("SETRANGE", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  final val strlen   = Command("STRLEN", StringInput, LongOutput)
}
