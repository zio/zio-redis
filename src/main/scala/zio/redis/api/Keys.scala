package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Keys {
  final val del      = RedisCommand("DEL", NonEmptyList(StringInput), LongOutput)
  final val dump     = RedisCommand("DUMP", StringInput, StringOutput)
  final val exists   = RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput)
  final val expire   = RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput)
  final val expireAt = RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput)
  final val keys     = RedisCommand("KEYS", StringInput, ChunkOutput)

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
    StringOutput
  )

  final val move      = RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  final val persist   = RedisCommand("PERSIST", StringInput, BoolOutput)
  final val pExpire   = RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput)
  final val pExpireAt = RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput)
  final val pTtl      = RedisCommand("PTTL", StringInput, DurationOutput)
  final val randomKey = RedisCommand("RANDOMKEY", NoInput, StringOutput)
  final val rename    = RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  final val renameNx  = RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput)

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
    StringOutput
  )

  final val scan = RedisCommand(
    "SCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val touch  = RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput)
  final val ttl    = RedisCommand("TTL", StringInput, DurationOutput)
  final val typeOf = RedisCommand("TYPE", StringInput, StringOutput)
  final val unlink = RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput)
  final val wait_  = RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput)
}
