package zio.redis

import Command.Input._
import Command.Output._

/*
 * TODO:
 *   - BLPOP (non-empty list not in last position)
 *   - BRPOP
 *   - LINSERT
 */
trait Lists {
  val brpoplpush = Command("BRPOPLPUSH", Tuple3(StringInput, StringInput, DurationInput), ByteOutput)
  val lindex     = Command("LINDEX", Tuple2(StringInput, LongInput), ByteOutput)
  val llen       = Command("LLEN", StringInput, LongOutput)
  val lpop       = Command("LPOP", StringInput, ByteOutput)
  val lpush      = Command("LPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val lpushx     = Command("LPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val lrange     = Command("LRANGE", Tuple2(StringInput, RangeInput), StreamOutput)
  val lrem       = Command("LREM", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
  val lset       = Command("LSET", Tuple3(StringInput, LongInput, ByteInput), UnitOutput)
  val ltrim      = Command("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  val rpop       = Command("RPOP", StringInput, ByteOutput)
  val rpoplpush  = Command("RPOPLPUSH", Tuple2(StringInput, StringInput), ByteOutput)
  val rpush      = Command("RPUSH", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
  val rpushx     = Command("RPUSHX", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)

}
