package zio

package object redis {
  import Command.Input._
  import Command.Output._

  /*
   * connection
   *
   * Problems:
   *   - is quit properly represented
   */
  lazy val auth   = Command("AUTH", StringInput, UnitOutput)
  lazy val echo   = Command("ECHO", StringInput, ValueOutput)
  lazy val ping   = Command("PING", Varargs(StringInput), ValueOutput)
  lazy val quit   = Command[Any, IO[Error, Unit]]("QUIT", NoInput, UnitOutput)
  lazy val select = Command("SELECT", LongInput, UnitOutput)
  lazy val swapDB = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)

  /*
   * hashes
   *
   * Problems:
   *   - varargs must be passed (is non-empty varargs a solution)
   *   - optional parameters in HSCAN
   *   - should we specialize value output for commands like HVALS
   */
  lazy val hDel         = Command("HDEL", Tuple3(KeyInput, StringInput, Varargs(StringInput)), LongOutput)
  lazy val hExists      = Command("HEXISTS", Tuple2(KeyInput, StringInput), BoolOutput)
  lazy val hGet         = Command("HGET", Tuple2(KeyInput, StringInput), ValueOutput)
  lazy val hGetAll      = Command("HGETALL", KeyInput, StreamOutput)
  lazy val hIncrBy      = Command("HINCRBY", Tuple3(KeyInput, StringInput, LongInput), LongOutput)
  lazy val hIncrByFloat = Command("HINCRBYFLOAT", Tuple3(KeyInput, StringInput, DoubleInput), ValueOutput)
  lazy val hKeys        = Command("HKEYS", KeyInput, StreamOutput)
  lazy val hLen         = Command("HLEN", KeyInput, LongOutput)
  lazy val hmGet        = Command("HMGET", Tuple3(KeyInput, StringInput, Varargs(StringInput)), StreamOutput)
  lazy val hSet = Command(
    "HSET",
    Tuple3(KeyInput, Tuple2(StringInput, StringInput), Varargs(Tuple2(StringInput, StringInput))),
    UnitOutput
  )
  lazy val hSetNX  = Command("HSETNX", Tuple3(KeyInput, StringInput, StringInput), BoolOutput)
  lazy val hStrLen = Command("HSTRLEN", Tuple2(KeyInput, StringInput), LongOutput)
  lazy val hVals   = Command("HVALS", KeyInput, ValueOutput)
}
