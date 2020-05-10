package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Keys {
  final val del       = RedisCommand("DEL", NonEmptyList(StringInput), LongOutput)
  final val dump      = RedisCommand("DUMP", StringInput, ByteOutput)
  final val exists    = RedisCommand("EXISTS", NonEmptyList(StringInput), BoolOutput)
  final val expire    = RedisCommand("EXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  final val expireAt  = RedisCommand("EXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  final val keys      = RedisCommand("KEYS", StringInput, ChunkOutput)
  final val move      = RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  final val persist   = RedisCommand("PERSIST", StringInput, BoolOutput)
  final val pExpire   = RedisCommand("PEXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
  final val pExpireAt = RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
  final val pTtl      = RedisCommand("PTTL", StringInput, DurationOutput)
  final val rename    = RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  final val renameNx  = RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput)

  final val scan = RedisCommand(
    "SCAN",
    Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
    ScanOutput
  )

  final val touch  = RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput)
  final val ttl    = RedisCommand("TTL", StringInput, DurationOutput)
  final val typeOf = RedisCommand("TYPE", StringInput, StringOutput)
  final val unlink = RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput)
}
