package zio.redis

import Command.Input._
import Command.Output._

trait Connection {
  val auth   = Command("AUTH", StringInput, UnitOutput)
  val echo   = Command("ECHO", StringInput, ByteOutput)
  val ping   = Command("PING", Varargs(StringInput), ByteOutput)
  val quit   = Command("QUIT", UnitInput, UnitOutput)
  val select = Command("SELECT", LongInput, UnitOutput)
  val swapdb = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)
}
