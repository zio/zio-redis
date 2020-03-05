package zio.redis

import Command.Input._
import Command.Output._

trait Hashes {
  val hdel         = Command("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  val hexists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  val hget         = Command("HGET", Tuple2(StringInput, StringInput), ByteOutput)
  val hgetall      = Command("HGETALL", StringInput, StreamOutput)
  val hincrby      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
  val hincrbyfloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ByteOutput)
  val hkeys        = Command("HKEYS", StringInput, StreamOutput)
  val hlen         = Command("HLEN", StringInput, LongOutput)
  val hmget        = Command("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), StreamOutput)
  val hscan = Command(
    "HSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  val hset = Command(
    "HSET",
    Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, ByteInput))),
    LongOutput
  )
  val hsetnx  = Command("HSETNX", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  val hstrlen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  val hvals   = Command("HVALS", StringInput, ByteOutput)

}
