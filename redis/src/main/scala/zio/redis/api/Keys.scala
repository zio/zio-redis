package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Keys {
  final val del      = RedisCommand("DEL", NonEmptyList(StringInput), LongOutput, Base)
  final val dump     = RedisCommand("DUMP", StringInput, MultiStringOutput, Base)
  final val exists   = RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput, Base)
  final val expire   = RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput, Base)
  final val expireAt = RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput, Base)
  final val keys     = RedisCommand("KEYS", StringInput, ChunkOutput, Base)

  final val migrate = RedisCommand(
    "MIGRATE",
    Tuple9(
      StringInput,
      LongInput,
      StringInput,
      LongInput,
      LongInput,
      OptionalInput(CopyInput),
      OptionalInput(ReplaceInput),
      OptionalInput(AuthInput),
      OptionalInput(NonEmptyList(StringInput))
    ),
    MultiStringOutput,
    Base
  )

  final val move      = RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput, Base)
  final val persist   = RedisCommand("PERSIST", StringInput, BoolOutput, Base)
  final val pExpire   = RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput, Base)
  final val pExpireAt = RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput, Base)
  final val pTtl      = RedisCommand("PTTL", StringInput, DurationMillisecondsOutput, Base)
  final val randomKey = RedisCommand("RANDOMKEY", NoInput, OptionalOutput(MultiStringOutput), Base)
  final val rename    = RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput, Base)
  final val renameNx  = RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput, Base)

  final val restore = RedisCommand(
    "RESTORE",
    Tuple7(
      StringInput,
      LongInput,
      StringInput,
      OptionalInput(ReplaceInput),
      OptionalInput(AbsTtlInput),
      OptionalInput(IdleTimeInput),
      OptionalInput(FreqInput)
    ),
    UnitOutput,
    Base
  )

  final val scan = RedisCommand(
    "SCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput,
    Base
  )

  final val touch  = RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput, Base)
  final val ttl    = RedisCommand("TTL", StringInput, DurationSecondsOutput, Base)
  final val typeOf = RedisCommand("TYPE", StringInput, TypeOutput, Base)
  final val unlink = RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput, Base)
  final val wait_  = RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput, Base)
}
