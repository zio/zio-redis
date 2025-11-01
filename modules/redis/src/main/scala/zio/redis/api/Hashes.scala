/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.api

import zio._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder._
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.schema.Schema

trait Hashes[G[+_]] extends RedisEnvironment[G] {
  import Hashes._

  /**
   * Removes the specified fields from the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be removed
   * @param field
   *   field to remove
   * @param fields
   *   additional fields
   * @return
   *   number of fields removed from the hash.
   */
  final def hDel[K: Schema, F: Schema](key: K, field: F, fields: F*): G[Long] = {
    val command =
      RedisCommand(HDel, Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[F]())), LongOutput)
    command.run((key, (field, fields.toList)))
  }

  /**
   * Returns if `field` is an existing field in the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be inspected
   * @param field
   *   field to inspect
   * @return
   *   true if the field exists, otherwise false.
   */
  final def hExists[K: Schema, F: Schema](key: K, field: F): G[Boolean] = {
    val command =
      RedisCommand(HExists, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()), BoolOutput)
    command.run((key, field))
  }

  /**
   * Returns the value associated with `field` in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose field should be read
   * @param field
   *   which value should be returned
   * @return
   *   value stored in the field, if any.
   */
  final def hGet[K: Schema, F: Schema](key: K, field: F): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[V: Schema]: G[Option[V]] =
        RedisCommand(
          HGet,
          Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()),
          OptionalOutput(ArbitraryOutput[V]())
        ).run((key, field))
    }

  /**
   * Returns all fields and values of the hash stored at `key`.
   *
   * @param key
   *   of the hash that should be read
   * @return
   *   map of `field -> value` pairs under the key.
   */
  final def hGetAll[K: Schema](key: K): ResultBuilder2[Map, G] = new ResultBuilder2[Map, G] {
    def returning[F: Schema, V: Schema]: G[Map[F, V]] = {
      val command =
        RedisCommand(
          HGetAll,
          ArbitraryKeyInput[K](),
          KeyValueOutput(ArbitraryOutput[F](), ArbitraryOutput[V]())
        )
      command.run(key)
    }
  }

  /**
   * Increments the number stored at `field` in the hash stored at `key` by `increment`. If field does not exist the
   * value is set to `increment`.
   *
   * @param key
   *   of the hash that should be updated
   * @param field
   *   field containing an integer, or a new field
   * @param increment
   *   integer to increment with
   * @return
   *   integer value after incrementing, or error if the field is not an integer.
   */
  final def hIncrBy[K: Schema, F: Schema](key: K, field: F, increment: Long): G[Long] = {
    val command =
      RedisCommand(HIncrBy, Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), LongInput), LongOutput)
    command.run((key, field, increment))
  }

  /**
   * Increment the specified `field` of a hash stored at `key`, and representing a floating point number by the
   * specified `increment`.
   *
   * @param key
   *   of the hash that should be updated
   * @param field
   *   field containing a float, or a new field
   * @param increment
   *   float to increment with
   * @return
   *   float value after incrementing, or error if the field is not a float.
   */
  final def hIncrByFloat[K: Schema, F: Schema](
    key: K,
    field: F,
    increment: Double
  ): G[Double] = {
    val command =
      RedisCommand(
        HIncrByFloat,
        Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), DoubleInput),
        DoubleOutput
      )
    command.run((key, field, increment))
  }

  /**
   * Returns all field names in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose fields should be returned
   * @return
   *   chunk of field names.
   */
  final def hKeys[K: Schema](key: K): ResultBuilder1[Chunk, G] =
    new ResultBuilder1[Chunk, G] {
      def returning[F: Schema]: G[Chunk[F]] =
        RedisCommand(HKeys, ArbitraryKeyInput[K](), ChunkOutput(ArbitraryOutput[F]())).run(key)
    }

  /**
   * Returns the number of fields contained in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose fields should be counted
   * @return
   *   number of fields.
   */
  final def hLen[K: Schema](key: K): G[Long] = {
    val command = RedisCommand(HLen, ArbitraryKeyInput[K](), LongOutput)
    command.run(key)
  }

  /**
   * Returns the values associated with the specified `fields` in the hash stored at `key`.
   *
   * @param key
   *   of the hash whose values should be read
   * @param field
   *   fields to retrieve
   * @param fields
   *   additional fields
   * @return
   *   chunk of values, where value is `None` if the field is not in the hash.
   */
  final def hmGet[K: Schema, F: Schema](
    key: K,
    field: F,
    fields: F*
  ): ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda, G] =
    new ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda, G] {
      def returning[V: Schema]: G[Chunk[Option[V]]] = {
        val command = RedisCommand(
          HmGet,
          Tuple2(ArbitraryKeyInput[K](), NonEmptyList(ArbitraryValueInput[F]())),
          ChunkOutput(OptionalOutput(ArbitraryOutput[V]()))
        )
        command.run((key, (field, fields.toList)))
      }
    }

  /**
   * Returns a random field from the hash value stored at `key`
   *
   * @param key
   *   of the hash which fields should be read
   * @return
   *   random field in the hash or `None` when `key` does not exist.
   */
  final def hRandField[K: Schema](key: K): ResultBuilder1[Option, G] =
    new ResultBuilder1[Option, G] {
      def returning[V: Schema]: G[Option[V]] =
        RedisCommand(HRandField, ArbitraryKeyInput[K](), OptionalOutput(ArbitraryOutput[V]())).run(key)
    }

  /**
   * Returns an array of at most `count` distinct fields randomly. If called with a negative `count`, the command will
   * return exactly `count` fields, allowing repeats. If `withValues` is true it will return fields with his values
   * intercalated.
   *
   * @param key
   *   of the hash which fields should be read
   * @param count
   *   maximum number of different fields to return if positive or the exact number, in absolute value if negative
   * @param withValues
   *   when true includes the respective values of the randomly selected hash fields
   * @return
   *   a list of fields or fields and values if `withValues` is true.
   */
  final def hRandField[K: Schema](key: K, count: Long, withValues: Boolean = false): ResultBuilder1[Chunk, G] =
    new ResultBuilder1[Chunk, G] {
      def returning[V: Schema]: G[Chunk[V]] = {
        val command = RedisCommand(
          HRandField,
          Tuple3(ArbitraryKeyInput[K](), LongInput, OptionalInput(StringInput)),
          ChunkOutput(ArbitraryOutput[V]())
        )
        command.run((key, count, if (withValues) Some("WITHVALUES") else None))
      }
    }

  /**
   * Iterates `fields` of Hash types and their associated values using a cursor-based iterator
   *
   * @param key
   *   of the hash that should be scanned
   * @param cursor
   *   integer representing iterator
   * @param pattern
   *   regular expression matching keys to scan
   * @param count
   *   approximate number of elements to return (see https://redis.io/commands/scan#the-count-option)
   * @return
   *   pair containing the next cursor and list of elements scanned.
   */
  final def hScan[K: Schema](
    key: K,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ResultBuilder2[({ type lambda[x, y] = (Long, Chunk[(x, y)]) })#lambda, G] =
    new ResultBuilder2[({ type lambda[x, y] = (Long, Chunk[(x, y)]) })#lambda, G] {
      def returning[F: Schema, V: Schema]: G[(Long, Chunk[(F, V)])] = {
        val command = RedisCommand(
          HScan,
          Tuple4(ArbitraryKeyInput[K](), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
          Tuple2Output(ArbitraryOutput[Long](), ChunkTuple2Output(ArbitraryOutput[F](), ArbitraryOutput[V]()))
        )
        command.run((key, cursor, pattern.map(Pattern(_)), count))
      }
    }

  /**
   * Sets `field -> value` pairs in the hash stored at `key`
   *
   * @param key
   *   of the hash whose value should be set
   * @param pair
   *   mapping of a field to value
   * @param pairs
   *   additional pairs
   * @return
   *   number of fields added.
   */
  final def hSet[K: Schema, F: Schema, V: Schema](
    key: K,
    pair: (F, V),
    pairs: (F, V)*
  ): G[Long] = {
    val command = RedisCommand(
      HSet,
      Tuple2(ArbitraryKeyInput[K](), NonEmptyList(Tuple2(ArbitraryValueInput[F](), ArbitraryValueInput[V]()))),
      LongOutput
    )
    command.run((key, (pair, pairs.toList)))
  }

  /**
   * Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist
   *
   * @param key
   *   of the hash whose value should be set
   * @param field
   *   for which a value should be set
   * @param value
   *   that should be set
   * @return
   *   true if `field` is a new field and value was set, otherwise false.
   */
  final def hSetNx[K: Schema, F: Schema, V: Schema](
    key: K,
    field: F,
    value: V
  ): G[Boolean] = {
    val command =
      RedisCommand(
        HSetNx,
        Tuple3(ArbitraryKeyInput[K](), ArbitraryValueInput[F](), ArbitraryValueInput[V]()),
        BoolOutput
      )
    command.run((key, field, value))
  }

  /**
   * Returns the string length of the value associated with `field` in the hash stored at `key`
   *
   * @param key
   *   of the hash that should be read
   * @param field
   *   which value length should be returned
   * @return
   *   string length of the value in field, or zero if either field or key do not exist.
   */
  final def hStrLen[K: Schema, F: Schema](key: K, field: F): G[Long] = {
    val command =
      RedisCommand(HStrLen, Tuple2(ArbitraryKeyInput[K](), ArbitraryValueInput[F]()), LongOutput)
    command.run((key, field))
  }

  /**
   * Returns all values in the hash stored at `key`
   *
   * @param key
   *   of the hash which values should be read
   * @return
   *   list of values in the hash, or an empty list when `key` does not exist.
   */
  final def hVals[K: Schema](key: K): ResultBuilder1[Chunk, G] =
    new ResultBuilder1[Chunk, G] {
      def returning[V: Schema]: G[Chunk[V]] =
        RedisCommand(HVals, ArbitraryKeyInput[K](), ChunkOutput(ArbitraryOutput[V]())).run(key)
    }
}

private[redis] object Hashes {
  final val HDel         = "HDEL"
  final val HExists      = "HEXISTS"
  final val HGet         = "HGET"
  final val HGetAll      = "HGETALL"
  final val HIncrBy      = "HINCRBY"
  final val HIncrByFloat = "HINCRBYFLOAT"
  final val HKeys        = "HKEYS"
  final val HLen         = "HLEN"
  final val HmGet        = "HMGET"
  final val HRandField   = "HRANDFIELD"
  final val HScan        = "HSCAN"
  final val HSet         = "HSET"
  final val HSetNx       = "HSETNX"
  final val HStrLen      = "HSTRLEN"
  final val HVals        = "HVALS"
}
