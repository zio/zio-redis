package zio.redis.api

import zio.redis.Command
import zio.redis.Input.{ NoInput, NonEmptyList, StringInput }
import zio.redis.Output.{ ChunkOutput, UnitOutput }

trait Transactions {
  final val discard = Command("DISCARD", NoInput, UnitOutput)
  final val exec    = Command("EXEC", NoInput, ChunkOutput)
  final val multi   = Command("MULTI", NoInput, UnitOutput)
  final val unwatch = Command("UNWATCH", NoInput, UnitOutput)
  final val watch   = Command("WATCH", NonEmptyList(StringInput), UnitOutput)
}
