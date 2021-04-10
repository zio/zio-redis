package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema

trait HyperLogLog {
  import HyperLogLog._

  /**
   *  Adds the specified elements to the specified HyperLogLog.
   *  @param key HLL key where the elements will be added
   *  @param element element to count
   *  @param elements additional elements to count
   *  @return boolean indicating if at least 1 HyperLogLog register was altered
   */
  final def pfAdd[K: Schema, V: Schema](key: K, element: V, elements: V*): ZIO[RedisExecutor, RedisError, Boolean] = {
    val command = RedisCommand(PfAdd, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[V]())), BoolOutput)
    command.run((key, (element, elements.toList)))
  }

  /**
   *  Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
   *  @param key HLL key
   *  @param keys additional keys
   *  @return approximate number of unique elements observed via PFADD
   */
  final def pfCount[K: Schema](key: K, keys: K*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(PfCount, NonEmptyList(ArbitraryInput[K]()), LongOutput)
    command.run((key, keys.toList))
  }

  /**
   *  Merge N different HyperLogLogs into a single one.
   *  @param destKey HLL key where the merged HLLs will be stored
   *  @param sourceKey HLL key to merge
   *  @param sourceKeys additional keys to merge
   */
  final def pfMerge[K: Schema](destKey: K, sourceKey: K, sourceKeys: K*): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(PfMerge, Tuple2(ArbitraryInput[K](), NonEmptyList(ArbitraryInput[K]())), UnitOutput)
    command.run((destKey, (sourceKey, sourceKeys.toList)))
  }
}

private[redis] object HyperLogLog {
  final val PfAdd   = "PFADD"
  final val PfCount = "PFCOUNT"
  final val PfMerge = "PFMERGE"
}
