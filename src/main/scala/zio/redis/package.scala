package zio

package object redis {
  import Command.Input._
  import Command.Output._

  lazy val append   = Command("APPEND", Tuple2(KeyInput, StringInput), UnitOutput)
  lazy val auth     = Command("AUTH", StringInput, UnitOutput)
  lazy val bitcount = Command("BITCOUNT", Tuple2(KeyInput, RangeInput), LongOutput)
  lazy val get      = Command("GET", KeyInput, ValueOutput)
  lazy val incr     = Command("INCR", KeyInput, LongOutput)
  lazy val incrBy   = Command("INCRBY", Tuple2(KeyInput, LongInput), LongOutput)
  lazy val lrange   = Command("LRANGE", Tuple2(KeyInput, RangeInput), StreamOutput)
}
