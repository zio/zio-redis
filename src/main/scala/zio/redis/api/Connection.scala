package zio.redis.api

import zio.redis.Command
import zio.redis.Command.Input._
import zio.redis.Command.Output._

trait Connection {
  val auth   = Command("AUTH", StringInput, UnitOutput)
  val echo   = Command("ECHO", StringInput, ByteOutput)
  val ping   = Command("PING", Varargs(StringInput), ByteOutput)
  val quit   = Command("QUIT", UnitInput, UnitOutput)
  val select = Command("SELECT", LongInput, UnitOutput)
  val swapdb = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)
}
