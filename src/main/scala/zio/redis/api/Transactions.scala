package zio.redis.api

import zio.redis.Command
import zio.redis.Input.{ NonEmptyList, StringInput, UnitInput }
import zio.redis.Output.{ StreamOutput, UnitOutput }

trait Transactions {
  final val discard = Command("DISCARD", UnitInput, UnitOutput)
  final val exec    = Command("EXEC", UnitInput, StreamOutput)
  final val multi   = Command("MULTI", UnitInput, UnitOutput)
  final val unwatch = Command("UNWATCH", UnitInput, UnitOutput)
  final val watch   = Command("WATCH", NonEmptyList(StringInput), UnitOutput)
}
