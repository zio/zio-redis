package zio.redis.api

import zio.redis.Command
import zio.redis.Input._
import zio.redis.Output._

trait Hashes {
  final val hdel         = Command("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val hexists      = Command("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  final val hget         = Command("HGET", Tuple2(StringInput, StringInput), ByteOutput)
  final val hgetall      = Command("HGETALL", StringInput, StreamOutput)
  final val hincrby      = Command("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)
  final val hincrbyfloat = Command("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), ByteOutput)
  final val hkeys        = Command("HKEYS", StringInput, StreamOutput)
  final val hlen         = Command("HLEN", StringInput, LongOutput)
  final val hmget        = Command("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), StreamOutput)
  final val hscan = Command(
    "HSCAN",
    Tuple4(LongInput, OptionalInput(MatchInput), OptionalInput(CountInput), OptionalInput(TypeInput)),
    ScanOutput
  )
  final val hset = Command(
    "HSET",
    Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, ByteInput))),
    LongOutput
  )
  final val hsetnx  = Command("HSETNX", Tuple3(StringInput, StringInput, ByteInput), BoolOutput)
  final val hstrlen = Command("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val hvals   = Command("HVALS", StringInput, ByteOutput)

}
