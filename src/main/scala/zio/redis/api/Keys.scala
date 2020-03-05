package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

/*
 * Problems:
 *   - optional parameters in MIGRATE
 *   - should we support OBJECT?
 *   - should we support RANDOMKEY?
 *   - should we support RESTORE?
 *   - optional parameters in SORT
 *   - should we support WAIT?
 */
trait Keys {
  val del       = Command("DEL", NonEmptyList(StringInput), LongOutput)
  val dump      = Command("DUMP", StringInput, ByteOutput)
  val exists    = Command("EXISTS", NonEmptyList(StringInput), LongOutput)
  val expire    = Command("EXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  val expireat  = Command("EXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  val keys      = Command("KEYS", StringInput, StreamOutput)
  val move      = Command("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  val persist   = Command("PERSIST", StringInput, BoolOutput)
  val pexpire   = Command("PEXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  val pexpireat = Command("PEXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  val pttl      = Command("PTTL", StringInput, DurationOutput)
  val rename    = Command("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  val renamenx  = Command("RENAMENX", Tuple2(StringInput, StringInput), UnitOutput)
  val scan = Command(
    "SCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  val touch  = Command("TOUCH", NonEmptyList(StringInput), LongOutput)
  val ttl    = Command("TTL", StringInput, DurationOutput)
  val `type` = Command("TYPE", StringInput, StringOutput)
  val unlink = Command("UNLINK", NonEmptyList(StringInput), LongOutput)
}
