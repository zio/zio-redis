package zio.redis.api

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Hashes {
  import Hashes._

  /**
   * Removes the specified fields from the hash stored at `key`.
   * @param key
   * @param field field to remove
   * @param fields additional fields
   * @return number of fields removed from the hash
   */
  final def hDel(key: String, field: String, fields: String*): ZIO[RedisExecutor, RedisError, Long] =
    HDel.run((key, (field, fields.toList)))

  /**
   * Returns if `field` is an existing field in the hash stored at `key`.
   * @param key
   * @param field field to inspect
   * @return boolean if the field exists
   */
  final def hExists(key: String, field: String): ZIO[RedisExecutor, RedisError, Boolean] = HExists.run((key, field))

  /**
   * Returns the value associated with `field` in the hash stored at `key`.
   * @param key
   * @param field
   * @return value stored in the field, if any
   */
  final def hGet(key: String, field: String): ZIO[RedisExecutor, RedisError, Option[String]] = HGet.run((key, field))

  /**
   * Returns all fields and values of the hash stored at `key`.
   * @param key
   * @return map of `field -> value` pairs under the key
   */
  final def hGetAll(key: String): ZIO[RedisExecutor, RedisError, Map[String, String]] = HGetAll.run(key)

  /**
   * Increments the number stored at `field` in the hash stored at `key` by `increment`. If field does not exist the
   * value is set to `increment`.
   * @param key
   * @param field field containing an integer, or a new field
   * @param increment integer to increment with
   * @return integer value after incrementing, or error if the field is not an integer
   */
  final def hIncrBy(key: String, field: String, increment: Long): ZIO[RedisExecutor, RedisError, Long] =
    HIncrBy.run((key, field, increment))

  /**
   * Increment the specified `field` of a hash stored at `key`, and representing a floating point number
   * by the specified `increment`.
   * @param key
   * @param field field containing a float, or a new field
   * @param increment float to increment with
   * @return float value after incrementing, or error if the field is not a float
   */
  final def hIncrByFloat(key: String, field: String, increment: Double): ZIO[RedisExecutor, RedisError, Double] =
    HIncrByFloat.run((key, field, increment))

  /**
   * Returns all field names in the hash stored at `key`.
   * @param key
   * @return chunk of field names
   */
  final def hKeys(key: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = HKeys.run(key)

  /**
   * Returns the number of fields contained in the hash stored at `key`.
   * @param key
   * @return number of fields
   */
  final def hLen(key: String): ZIO[RedisExecutor, RedisError, Long] = HLen.run(key)

  /**
   * Returns the values associated with the specified `fields` in the hash stored at `key`.
   * @param key
   * @param field fields to retrieve
   * @param fields additional fields
   * @return chunk of values, where value is `None` if the field is not in the hash
   */
  final def hmGet(key: String, field: String, fields: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
    HmGet.run((key, (field, fields.toList)))

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

  /**
   * Iterates `fields` of Hash types and their associated values.
   * @param key
   * @param cursor
   * @param pattern
   * @param count
   * @return
   */
  final def hScan(
    key: String,
    cursor: Long,
    pattern: Option[Regex] = None,
    count: Option[Long] = None,
    `type`: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, (Long, Chunk[String])] = HScan.run((key, cursor, pattern, count, `type`))

  /**
   * Sets `field -> value` pairs in the hash stored at `key`.
   * @param key
   * @param pair
   * @param pairs
   * @return
   */
  final def hSet(key: String, pair: (String, String), pairs: (String, String)*): ZIO[RedisExecutor, RedisError, Long] =
    HSet.run((key, (pair, pairs.toList)))

  /**
   * Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist
   * @param key
   * @param field
   * @param value
   * @return
   */
  final def hSetNx(key: String, field: String, value: String): ZIO[RedisExecutor, RedisError, Boolean] =
    HSetNx.run((key, field, value))

  /**
   * Returns the string length of the value associated with `field` in the hash stored at `key`.
   * @param key
   * @param field
   * @return
   */
  final def hStrLen(key: String, field: String): ZIO[RedisExecutor, RedisError, Long] = HStrLen.run((key, field))

  /**
   * Returns all values in the hash stored at `key`.
   * @param key
   * @return
   */
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
      Tuple5(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val HSet =
    RedisCommand("HSET", Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))), LongOutput)

  final val HSetNx  = RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val HStrLen = RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val HVals   = RedisCommand("HVALS", StringInput, ChunkOutput)
}
