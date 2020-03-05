package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Keys {
  final val del       = Command("DEL", NonEmptyList(StringInput), LongOutput)
  final val dump      = Command("DUMP", StringInput, ByteOutput)
  final val exists    = Command("EXISTS", NonEmptyList(StringInput), LongOutput)
  final val expire    = Command("EXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  final val expireat  = Command("EXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  final val keys      = Command("KEYS", StringInput, StreamOutput)
  final val move      = Command("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  final val persist   = Command("PERSIST", StringInput, BoolOutput)
  final val pexpire   = Command("PEXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  final val pexpireat = Command("PEXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  final val pttl      = Command("PTTL", StringInput, DurationOutput)
  final val rename    = Command("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  final val renamenx  = Command("RENAMENX", Tuple2(StringInput, StringInput), UnitOutput)
  final val scan = Command(
    "SCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  final val touch  = Command("TOUCH", NonEmptyList(StringInput), LongOutput)
  final val ttl    = Command("TTL", StringInput, DurationOutput)
  final val `type` = Command("TYPE", StringInput, StringOutput)
  final val unlink = Command("UNLINK", NonEmptyList(StringInput), LongOutput)
}
