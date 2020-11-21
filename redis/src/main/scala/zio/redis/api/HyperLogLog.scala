package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait HyperLogLog {
  import HyperLogLog._

  /** Adds the specified elements to the specified HyperLogLog. */
  final def pfAdd(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Boolean] =
    PfAdd.run((key, (element, elements.toList)))

  /** Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s). */
  final def pfCount(key: String, keys: String*): ZIO[RedisExecutor, RedisError, Long] = PfCount.run((key, keys.toList))

  /** Merge N different HyperLogLogs into a single one. */
  final def pfMerge(destKey: String, sourceKey: String, sourceKeys: String*): ZIO[RedisExecutor, RedisError, Unit] =
    PfMerge.run((destKey, (sourceKey, sourceKeys.toList)))
}

private object HyperLogLog {
  final val PfAdd   = RedisCommand("PFADD", Tuple2(StringInput, NonEmptyList(StringInput)), BoolOutput)
  final val PfCount = RedisCommand("PFCOUNT", NonEmptyList(StringInput), LongOutput)
  final val PfMerge = RedisCommand("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
}
