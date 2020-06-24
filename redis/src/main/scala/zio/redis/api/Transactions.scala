package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input.{ NoInput, NonEmptyList, StringInput }
import zio.redis.Output.{ ChunkOutput, UnitOutput }

trait Transactions {
  final val discard = RedisCommand("DISCARD", NoInput, UnitOutput)
  final val exec    = RedisCommand("EXEC", NoInput, ChunkOutput)
  final val multi   = RedisCommand("MULTI", NoInput, UnitOutput)
  final val unwatch = RedisCommand("UNWATCH", NoInput, UnitOutput)
  final val watch   = RedisCommand("WATCH", NonEmptyList(StringInput), UnitOutput)
}
