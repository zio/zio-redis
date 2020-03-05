package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

/*
 * TODO:
 *   - BITFIELD
 *   - BITOP
 *   - BITPOS
 *   - SET
 */
trait Strings {
  val append      = Command("APPEND", Tuple2(StringInput, ByteInput), LongOutput)
  val bitcount    = Command("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)
  val decr        = Command("DECR", StringInput, LongOutput)
  val decrby      = Command("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  val get         = Command("GET", StringInput, ByteOutput)
  val getbit      = Command("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  val getrange    = Command("GETRANGE", Tuple2(StringInput, RangeInput), ByteOutput)
  val getset      = Command("GETSET", Tuple2(StringInput, ByteInput), ByteOutput)
  val incr        = Command("INCR", StringInput, LongOutput)
  val incrby      = Command("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  val incrbyfloat = Command("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), ByteOutput)
  val mget        = Command("MGET", NonEmptyList(StringInput), StreamOutput)
  val mset        = Command("MSET", NonEmptyList(Tuple2(StringInput, ByteInput)), UnitOutput)
  val msetnx      = Command("MSETNX", NonEmptyList(Tuple2(StringInput, ByteInput)), BoolOutput)
  val psetex      = Command("PSETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
  val setbit      = Command("SETBIT", Tuple3(StringInput, LongInput, ByteInput), ByteOutput)
  val setex       = Command("SETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
  val setnx       = Command("SETNX", Tuple2(StringInput, ByteInput), BoolOutput)
  val setrange    = Command("SETRANGE", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  val strlen      = Command("STRLEN", StringInput, LongOutput)
}
