package zio

package object redis {
  import Command.Input._
  import Command.Output._

  object connection {
    lazy val auth   = Command("AUTH", StringInput, UnitOutput)
    lazy val echo   = Command("ECHO", StringInput, ValueOutput)
    lazy val ping   = Command("PING", Varargs(StringInput), ValueOutput)
    lazy val quit   = Command("QUIT", NoInput, UnitOutput)
    lazy val select = Command("SELECT", LongInput, UnitOutput)
    lazy val swapdb = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)
  }

  /*
   * Hashes
   *
   * Problems:
   *   - optional parameters in HSCAN
   *   - should we specialize value output for commands like HVALS
   *   - should we generalize fields and keys?
   *
   * TODO:
   *   - keys as chunks
   *   - implicit conversion fron string to chunk
   *   - Chunk.fromString (maybe)
   */
  object hashes {
    lazy val hdel         = Command("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val hexists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
    lazy val hget         = Command("HGET", Tuple2(StringInput, StringInput), ValueOutput)
    lazy val hgetall      = Command("HGETALL", StringInput, StreamOutput)
    lazy val hincrby      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
    lazy val hincrbyfloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ValueOutput)
    lazy val hkeys        = Command("HKEYS", StringInput, StreamOutput)
    lazy val hlen         = Command("HLEN", StringInput, LongOutput)
    lazy val hmget        = Command("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), StreamOutput)
    lazy val hset = Command(
      "HSET",
      Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, ValueInput))),
      LongOutput
    )
    lazy val hsetnx  = Command("HSETNX", Tuple3(StringInput, StringInput, ValueInput), BoolOutput)
    lazy val hstrlen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
    lazy val hvals   = Command("HVALS", StringInput, ValueOutput)
  }

  /*
   * Keys
   *
   * Problems:
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
  object keys {
    lazy val del       = Command("DEL", NonEmptyList(StringInput), LongOutput)
    lazy val dump      = Command("DUMP", StringInput, ValueOutput)
    lazy val exists    = Command("EXISTS", NonEmptyList(StringInput), LongOutput)
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
    lazy val touch     = Command("TOUCH", NonEmptyList(StringInput), LongOutput)
    lazy val ttl       = Command("TTL", StringInput, LongOutput)
    lazy val `type`    = Command("TYPE", StringInput, StringOutput)
    lazy val unlink    = Command("UNLINK", NonEmptyList(StringInput), LongOutput)
  }

  /*
   * Sets
   *
   * Problems:
   *   - optional parameters in SPOP and SRANDMEMBER
   *   - optional parameters in SSCAN
   */
  object sets {
    lazy val sadd        = Command("SADD", Tuple2(StringInput, NonEmptyList(ValueInput)), LongOutput)
    lazy val scard       = Command("SCARD", StringInput, LongOutput)
    lazy val sdiff       = Command("SDIFF", NonEmptyList(StringInput), ValueOutput)
    lazy val sdiffstore  = Command("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val sinter      = Command("SINTER", NonEmptyList(StringInput), ValueOutput)
    lazy val sinterstore = Command("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val sismember   = Command("SISMEMBER", Tuple2(StringInput, ValueInput), BoolOutput)
    lazy val smembers    = Command("SMEMBERS", StringInput, ValueOutput)
    lazy val smove       = Command("SMOVE", Tuple3(StringInput, StringInput, ValueInput), BoolOutput)
    lazy val spop        = Command("SPOP", Tuple2(StringInput, LongInput), ValueOutput)
    lazy val srandmember = Command("SRANDMEMBER", Tuple2(StringInput, LongInput), ValueOutput)
    lazy val srem        = Command("SREM", Tuple2(StringInput, NonEmptyList(ValueInput)), LongOutput)
    lazy val sunion      = Command("SUNION", NonEmptyList(StringInput), ValueOutput)
    lazy val sunionstore = Command("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  }
}
