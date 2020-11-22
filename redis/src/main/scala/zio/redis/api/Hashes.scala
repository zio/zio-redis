package zio.redis.api

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Hashes {
  import Hashes._

  /** Removes the specified fields from the hash stored at `key`. */
  final def hDel(key: String, field: String, fields: String*): ZIO[RedisExecutor, RedisError, Long] =
    HDel.run((key, (field, fields.toList)))

  /** Returns if `field` is an existing field in the hash stored at `key`. */
  final def hExists(key: String, field: String): ZIO[RedisExecutor, RedisError, Boolean] = HExists.run((key, field))

  /** Returns the value associated with `field` in the hash stored at `key`. */
  final def hGet(key: String, value: String): ZIO[RedisExecutor, RedisError, Option[String]] = HGet.run((key, value))

  /** Returns all fields and values of the hash stored at `key`. */
  final def hGetAll(key: String): ZIO[RedisExecutor, RedisError, Map[String, String]] = HGetAll.run(key)

  /** Increments the number stored at `field` in the hash stored at `key` by `increment`. */
  final def hIncrBy(key: String, value: String, increment: Long): ZIO[RedisExecutor, RedisError, Long] =
    HIncrBy.run((key, value, increment))

  /** Increment the specified `field` of a hash stored at `key`, and representing a floating point number,
   *  by the specified `increment`. */
  final def hIncrByFloat(key: String, value: String, increment: Double): ZIO[RedisExecutor, RedisError, Double] =
    HIncrByFloat.run((key, value, increment))

  /** Returns all field names in the hash stored at `key`. */
  final def hKeys(key: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = HKeys.run(key)

  /** Returns the number of fields contained in the hash stored at `key`. */
  final def hLen(key: String): ZIO[RedisExecutor, RedisError, Long] = HLen.run(key)

  /** Returns the values associated with the specified `fields` in the hash stored at `key`. */
  final def hmGet(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
    HmGet.run((a, (b, bs.toList)))
  
  /**
   *  Sets the specified `field -> value` pairs in the hash stored at `key`.
   *  Deprecated: As per Redis 4.0.0, HMSET is considered deprecated. Please use `hSet` instead.
   *  @param key hash key
   *  @param pair mapping of a field to value
   *  @param pairs additional pairs
   *  @return unit if fields are successfully set
   */
  final def hmSet(
    key: String,
    pair: (String, String),
    pairs: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Unit] =
    HmSet.run((key, (pair, pairs.toList)))
  /** Iterates `fields` of Hash types and their associated values. */
  final def hScan(
    cursor: Long,
    pattern: Option[Regex] = None,
    count: Option[Long] = None,
    `type`: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = HScan.run((cursor, pattern, count, `type`))

  final def hScan(
    a: Long,
    b: Option[Regex] = None,
    c: Option[Long] = None,
    d: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, (Long, Chunk[String])] = HScan.run((a, b, c, d))

  /** Sets `field -> value` pairs in the hash stored at `key`. */
  final def hSet(key: String, pair: (String, String), pairs: (String, String)*): ZIO[RedisExecutor, RedisError, Long] =
    HSet.run((key, (pair, pairs.toList)))

  /** Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist.*/
  final def hSetNx(key: String, field: String, value: String): ZIO[RedisExecutor, RedisError, Boolean] =
    HSetNx.run((key, field, value))

  /** Returns the string length of the value associated with `field` in the hash stored at `key`. */
  final def hStrLen(key: String, field: String): ZIO[RedisExecutor, RedisError, Long] = HStrLen.run((key, field))

  /** Returns all values in the hash stored at `key`. */
  final def hVals(key: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = HVals.run(key)
}

private[redis] object Hashes {
  final val HDel    = RedisCommand("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val HExists = RedisCommand("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  final val HGet    = RedisCommand("HGET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val HGetAll = RedisCommand("HGETALL", StringInput, KeyValueOutput)
  final val HIncrBy = RedisCommand("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)

  final val HIncrByFloat =
    RedisCommand("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), DoubleOutput)

  final val HKeys = RedisCommand("HKEYS", StringInput, ChunkOutput)
  final val HLen  = RedisCommand("HLEN", StringInput, LongOutput)
  final val HmGet =
    RedisCommand("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOptionalMultiStringOutput)

  final val HmSet =
    RedisCommand(
      "HMSET",
      Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))),
      UnitOutput
    )

  final val HScan =
    RedisCommand(
      "HSCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val HSet =
    RedisCommand("HSET", Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))), LongOutput)

  final val HSetNx  = RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val HStrLen = RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val HVals   = RedisCommand("HVALS", StringInput, ChunkOutput)
}
