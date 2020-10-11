package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait HyperLogLog {
  import HyperLogLog._

  final def pfAdd(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Boolean] =
    PfAdd.run((a, (b, bs.toList)))

  final def pfCount(a: String, as: String*): ZIO[RedisExecutor, RedisError, Long] = PfCount.run((a, as.toList))

  final def pfMerge(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Unit] =
    PfMerge.run((a, (b, bs.toList)))
}

private[api] object HyperLogLog {
  final val PfAdd   = RedisCommand("PFADD", Tuple2(StringInput, NonEmptyList(StringInput)), BoolOutput)
  final val PfCount = RedisCommand("PFCOUNT", NonEmptyList(StringInput), LongOutput)
  final val PfMerge = RedisCommand("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
}
