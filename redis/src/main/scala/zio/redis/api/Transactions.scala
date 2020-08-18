package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input.{ NoInput, NonEmptyList, StringInput }
import zio.redis.Output.{ ChunkOutput, UnitOutput }
import zio.redis.RedisCommand

trait Transactions {
  final val discard = RedisCommand("DISCARD", NoInput, UnitOutput, Base)
  final val exec    = RedisCommand("EXEC", NoInput, ChunkOutput, Base)
  final val multi   = RedisCommand("MULTI", NoInput, UnitOutput, Base)
  final val unwatch = RedisCommand("UNWATCH", NoInput, UnitOutput, Base)
  final val watch   = RedisCommand("WATCH", NonEmptyList(StringInput), UnitOutput, Base)
}
