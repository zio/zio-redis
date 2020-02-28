package zio

package object redis {

  import Command.Input._
  import Command.Output._

  object connection {
    lazy val auth   = Command("AUTH", StringInput, UnitOutput)
    lazy val echo   = Command("ECHO", StringInput, ByteOutput)
    lazy val ping   = Command("PING", Varargs(StringInput), ByteOutput)
    lazy val quit   = Command("QUIT", UnitInput, UnitOutput)
    lazy val select = Command("SELECT", LongInput, UnitOutput)
    lazy val swapdb = Command("SWAPDB", Tuple2(LongInput, LongInput), UnitOutput)
  }

  object hashes {
    lazy val hdel         = Command("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val hexists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
    lazy val hget         = Command("HGET", Tuple2(StringInput, StringInput), ByteOutput)
    lazy val hgetall      = Command("HGETALL", StringInput, StreamOutput)
    lazy val hincrby      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
    lazy val hincrbyfloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ByteOutput)
    lazy val hkeys        = Command("HKEYS", StringInput, StreamOutput)
    lazy val hlen         = Command("HLEN", StringInput, LongOutput)
    lazy val hmget        = Command("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), StreamOutput)
    lazy val hscan = Command(
      "HSCAN",
      Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
      ScanOutput
    )
    lazy val hset = Command(
      "HSET",
      Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, ByteInput))),
      LongOutput
    )
    lazy val hsetnx  = Command("HSETNX", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
    lazy val hstrlen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
    lazy val hvals   = Command("HVALS", StringInput, ByteOutput)
  }

  object hll {
    lazy val pfadd   = Command("PFADD", Tuple2(StringInput, NonEmptyList(ByteInput)), BoolOutput)
    lazy val pfcount = Command("PFCOUNT", NonEmptyList(StringInput), LongOutput)
    lazy val pfmerge = Command("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
  }

  /*
   * Keys
   *
   * Problems:
   *   - optional parameters in MIGRATE
   *   - should we support OBJECT?
   *   - should we support RANDOMKEY?
   *   - should we support RESTORE?
   *   - optional parameters in SORT
   *   - should we support WAIT?
   */
  object keys {
    lazy val del       = Command("DEL", NonEmptyList(StringInput), LongOutput)
    lazy val dump      = Command("DUMP", StringInput, ByteOutput)
    lazy val exists    = Command("EXISTS", NonEmptyList(StringInput), LongOutput)
    lazy val expire    = Command("EXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
    lazy val expireat  = Command("EXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
    lazy val keys      = Command("KEYS", StringInput, StreamOutput)
    lazy val move      = Command("MOVE", Tuple2(StringInput, LongInput), BoolOutput)
    lazy val persist   = Command("PERSIST", StringInput, BoolOutput)
    lazy val pexpire   = Command("PEXPIRE", Tuple2(StringInput, DurationInput), BoolOutput)
    lazy val pexpireat = Command("PEXPIREAT", Tuple2(StringInput, TimeInput), BoolOutput)
    lazy val pttl      = Command("PTTL", StringInput, DurationOutput)
    lazy val rename    = Command("RENAME", Tuple2(StringInput, StringInput), UnitOutput)
    lazy val renamenx  = Command("RENAMENX", Tuple2(StringInput, StringInput), UnitOutput)
    lazy val scan = Command(
      "SCAN",
      Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
      ScanOutput
    )
    lazy val touch  = Command("TOUCH", NonEmptyList(StringInput), LongOutput)
    lazy val ttl    = Command("TTL", StringInput, DurationOutput)
    lazy val `type` = Command("TYPE", StringInput, StringOutput)
    lazy val unlink = Command("UNLINK", NonEmptyList(StringInput), LongOutput)
  }

  object sets {
    lazy val sadd        = Command("SADD", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
    lazy val scard       = Command("SCARD", StringInput, LongOutput)
    lazy val sdiff       = Command("SDIFF", NonEmptyList(StringInput), ByteOutput)
    lazy val sdiffstore  = Command("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val sinter      = Command("SINTER", NonEmptyList(StringInput), ByteOutput)
    lazy val sinterstore = Command("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    lazy val sismember   = Command("SISMEMBER", Tuple2(StringInput, ByteInput), BoolOutput)
    lazy val smembers    = Command("SMEMBERS", StringInput, ByteOutput)
    lazy val smove       = Command("SMOVE", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
    lazy val spop        = Command("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
    lazy val srandmember = Command("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), ByteOutput)
    lazy val srem        = Command("SREM", Tuple2(StringInput, NonEmptyList(ByteInput)), LongOutput)
    lazy val sscan = Command(
      "SSCAN",
      Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
      ScanOutput
    )
    lazy val touch       = Command("TOUCH", NonEmptyList(StringInput), LongOutput)
    lazy val sunion      = Command("SUNION", NonEmptyList(StringInput), ByteOutput)
    lazy val sunionstore = Command("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  }

  /*
   * TODO:
   *   - BITFIELD
   *   - BITOP
   *   - BITPOS
   *   - SET
   */
  object strings {
    lazy val append      = Command("APPEND", Tuple2(StringInput, ByteInput), LongOutput)
    lazy val bitcount    = Command("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)
    lazy val decr        = Command("DECR", StringInput, LongOutput)
    lazy val decrby      = Command("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
    lazy val get         = Command("GET", StringInput, ByteOutput)
    lazy val getbit      = Command("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
    lazy val getrange    = Command("GETRANGE", Tuple2(StringInput, RangeInput), ByteOutput)
    lazy val getset      = Command("GETSET", Tuple2(StringInput, ByteInput), ByteOutput)
    lazy val incr        = Command("INCR", StringInput, LongOutput)
    lazy val incrby      = Command("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
    lazy val incrbyfloat = Command("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), ByteOutput)
    lazy val mget        = Command("MGET", NonEmptyList(StringInput), StreamOutput)
    lazy val mset        = Command("MSET", NonEmptyList(Tuple2(StringInput, ByteInput)), UnitOutput)
    lazy val msetnx      = Command("MSETNX", NonEmptyList(Tuple2(StringInput, ByteInput)), BoolOutput)
    lazy val psetex      = Command("PSETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
    lazy val setbit      = Command("SETBIT", Tuple3(StringInput, LongInput, ByteInput), ByteOutput)
    lazy val setex       = Command("SETEX", Tuple3(StringInput, DurationInput, ByteInput), UnitOutput)
    lazy val setnx       = Command("SETNX", Tuple2(StringInput, ByteInput), BoolOutput)
    lazy val setrange    = Command("SETRANGE", Tuple3(StringInput, LongInput, ByteInput), LongOutput)
    lazy val strlen      = Command("STRLEN", StringInput, LongOutput)
  }
}
