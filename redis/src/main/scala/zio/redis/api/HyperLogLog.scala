package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait HyperLogLog {
  import HyperLogLog._

  /**
   *  Adds the specified elements to the specified HyperLogLog.
   *  @param key HLL key where the elements will be added
   *  @param element element to count
   *  @param elements additional elements to count
   *  @return boolean indicating if at least 1 HyperLogLog register was altered
   */
  final def pfAdd(key: String, element: String, elements: String*): ZIO[RedisExecutor, RedisError, Boolean] =
    PfAdd.run((key, (element, elements.toList)))

  /**
   *  Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
   *  @param key HLL key
   *  @param keys additional keys
   *  @return approximate number of unique elements observed via PFADD
   */
  final def pfCount(key: String, keys: String*): ZIO[RedisExecutor, RedisError, Long] = PfCount.run((key, keys.toList))

  /**
   *  Merge N different HyperLogLogs into a single one.
   *  @param destKey HLL key where the merged HLLs will be stored
   *  @param sourceKey HLL key to merge
   *  @param sourceKeys additional keys to merge
   */
  final def pfMerge(destKey: String, sourceKey: String, sourceKeys: String*): ZIO[RedisExecutor, RedisError, Unit] =
    PfMerge.run((destKey, (sourceKey, sourceKeys.toList)))
}

private[redis] object HyperLogLog {
  final val PfAdd   = RedisCommand("PFADD", Tuple2(StringInput, NonEmptyList(StringInput)), BoolOutput)
  final val PfCount = RedisCommand("PFCOUNT", NonEmptyList(StringInput), LongOutput)
  final val PfMerge = RedisCommand("PFMERGE", Tuple2(StringInput, NonEmptyList(StringInput)), UnitOutput)
}
