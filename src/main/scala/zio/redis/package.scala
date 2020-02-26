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
  lazy val swapdb = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)

  /*
   * hashes
   *
   * Problems:
   *   - varargs must be passed (is non-empty varargs a solution)
   *   - optional parameters in HSCAN
   *   - should we specialize value output for commands like HVALS
   *   - should we generalize fields and keys?
   */
  lazy val hdel         = Command("HDEL", Tuple3(StringInput, StringInput, Varargs(StringInput)), LongOutput)
  lazy val hexists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  lazy val hget         = Command("HGET", Tuple2(StringInput, StringInput), ValueOutput)
  lazy val hgetall      = Command("HGETALL", StringInput, StreamOutput)
  lazy val hincrby      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
  lazy val hincrbyfloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ValueOutput)
  lazy val hkeys        = Command("HKEYS", StringInput, StreamOutput)
  lazy val hlen         = Command("HLEN", StringInput, LongOutput)
  lazy val hmget        = Command("HMGET", Tuple3(StringInput, StringInput, Varargs(StringInput)), StreamOutput)
  lazy val hset = Command(
    "HSET",
    Tuple3(StringInput, Tuple2(StringInput, ValueInput), Varargs(Tuple2(StringInput, ValueInput))),
    LongOutput
  )
  lazy val hsetnx  = Command("HSETNX", Tuple3(StringInput, StringInput, ValueInput), BoolOutput)
  lazy val hstrlen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  lazy val hvals   = Command("HVALS", StringInput, ValueOutput)

  /*
   * keys
   *
   * Problems:
   *   - varargs must be passed (is non-empty varargs a solution)
   *   - should EXPIREAT and PEXPIREAT accept Date (lettuce approach)?
   *   - optional parameters in MIGRATE
   *   - should we support OBJECT?
   *   - should we refine response to commands like PTTL (e.g. -2 one error, -1 another error etc.)
   *   - should we support RANDOMKEY?
   *   - should we support RESTORE?
   *   - optional parameters in SCAN
   *   - optional parameters in SORT
   *   - should we support WAIT?
   */
  lazy val del       = Command("DEL", Tuple2(StringInput, Varargs(StringInput)), LongOutput)
  lazy val dump      = Command("DUMP", StringInput, ValueOutput)
  lazy val exists    = Command("EXISTS", Tuple2(StringInput, Varargs(StringInput)), LongOutput)
  lazy val expire    = Command("EXPIRE", Tuple2(StringInput, LongInput), BoolOutput)
  lazy val expireat  = Command("EXPIREAT", Tuple2(StringInput, LongInput), BoolOutput)
  lazy val keys      = Command("KEYS", StringInput, StreamOutput)
  lazy val move      = Command("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
  lazy val persist   = Command("PERSIST", StringInput, BoolOutput)
  lazy val pexpire   = Command("PEXPIRE", Tuple2(StringInput, LongInput), BoolOutput)
  lazy val pexpireat = Command("PEXPIREAT", Tuple2(StringInput, LongInput), BoolOutput)
  lazy val pttl      = Command("PTTL", StringInput, LongOutput)
  lazy val rename    = Command("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
  lazy val renamenx  = Command("RENAMENX", Tuple2(StringInput, StringInput), UnitOutput)
  lazy val touch     = Command("TOUCH", Tuple2(StringInput, Varargs(StringInput)), LongOutput)
  lazy val ttl       = Command("TTL", StringInput, LongOutput)
  lazy val `type`    = Command("TYPE", StringInput, StringOutput)
  lazy val unlink    = Command("UNLINK", Tuple2(StringInput, Varargs(StringInput)), LongOutput)
}
