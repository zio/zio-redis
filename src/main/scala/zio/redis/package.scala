package zio

package object redis {
  import Command.Input._
  import Command.Output._

  // connection
  lazy val auth   = Command("AUTH", StringInput, UnitOutput)
  lazy val echo   = Command("ECHO", StringInput, ValueOutput)
  lazy val ping   = Command("PING", Varargs(StringInput), ValueOutput)
  lazy val quit   = Command[Any, IO[Error, Unit]]("QUIT", NoInput, UnitOutput)
  lazy val select = Command("SELECT", LongInput, UnitOutput)
  lazy val swapDB = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)
}
