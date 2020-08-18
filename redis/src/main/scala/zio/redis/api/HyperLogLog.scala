package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait HyperLogLog {
  final val pfAdd   = RedisCommand("PFADD", Tuple2(StringInput, NonEmptyList(StringInput)), BoolOutput, Base)
  final val pfCount = RedisCommand("PFCOUNT", NonEmptyList(StringInput), LongOutput, Base)
  final val pfMerge = RedisCommand("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput, Base)
}
