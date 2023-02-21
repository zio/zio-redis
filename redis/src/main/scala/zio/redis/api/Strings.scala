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
import zio.redis.ResultBuilder._
import zio.redis._
import zio.schema.Schema

import java.time.Instant

trait Strings extends commands.Strings {

  /**
   * Append a value to a key.
   *
   * @param key
   *   Key of the string to add the value to
   * @param value
   *   Value to append to the string
   * @return
   *   Returns the length of the string after the append operation.
   */
  final def append[K: Schema, V: Schema](key: K, value: V): IO[RedisError, Long] = _append[K, V].run((key, value))

  /**
   * Count set bits in a string.
   *
   * @param key
   *   Key of the string of which to count the bits
   * @param range
   *   Range of bytes to count
   * @return
   *   Returns the number of bits set to 1.
   */
  final def bitCount[K: Schema](key: K, range: Option[Range] = None): IO[RedisError, Long] =
    _bitCount[K].run((key, range))

  /**
   * Perform arbitrary bitfield integer operations on strings.
   *
   * @param key
   *   Key of the string to operate on
   * @param bitFieldCommand
   *   First command to apply
   * @param bitFieldCommands
   *   Subsequent commands to apply
   * @return
   *   Returns an optional long result of each command applied.
   */
  final def bitField[K: Schema](
    key: K,
    bitFieldCommand: BitFieldCommand,
    bitFieldCommands: BitFieldCommand*
  ): IO[RedisError, Chunk[Option[Long]]] =
    _bitField[K].run((key, (bitFieldCommand, bitFieldCommands.toList)))

  /**
   * Perform bitwise operations between strings.
   *
   * @param operation
   *   Bit operation to apply
   * @param destKey
   *   Key of destination string to store the result
   * @param srcKey
   *   First source key to apply the operation to
   * @param srcKeys
   *   Subsequent source keys to apply the operation to
   * @return
   *   Returns size of the string stored in the destination key, that is equal to the size of the longest input string.
   */
  final def bitOp[D: Schema, S: Schema](
    operation: BitOperation,
    destKey: D,
    srcKey: S,
    srcKeys: S*
  ): IO[RedisError, Long] =
    _bitOp[D, S].run((operation, destKey, (srcKey, srcKeys.toList)))

  /**
   * Find first bit set or clear in a string.
   *
   * @param key
   *   Key of the string to search within
   * @param bit
   *   Whether to search for a set bit or a cleared bit
   * @param range
   *   Range of bytes to search
   * @return
   *   Returns the position of the first bit set to 1 or 0 according to the request.
   */
  final def bitPos[K: Schema](key: K, bit: Boolean, range: Option[BitPosRange] = None): IO[RedisError, Long] =
    _bitPos[K].run((key, bit, range))

  /**
   * Decrement the integer value of a key by one.
   *
   * @param key
   *   Key to decrement
   * @return
   *   Returns the value of key after the decrement.
   */
  final def decr[K: Schema](key: K): IO[RedisError, Long] = _decr[K].run(key)

  /**
   * Decrement the integer value of a key by the given number.
   *
   * @param key
   *   Key of the integer value to decrement
   * @param decrement
   *   Amount to decrement by
   * @return
   *   Returns the value of key after the decrement.
   */
  final def decrBy[K: Schema](key: K, decrement: Long): IO[RedisError, Long] = _decrBy[K].run((key, decrement))

  /**
   * Get the value of a key.
   *
   * @param key
   *   Key to get the value of
   * @return
   *   Returns the value of the string or None if it does not exist.
   */
  final def get[K: Schema](key: K): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _get[K, R].run(key)
    }

  /**
   * Returns the bit value at offset in the string value stored at key.
   *
   * @param key
   *   Key of the string to get the bit from
   * @param offset
   *   Offset to the bit
   * @return
   *   Returns the bit value stored at offset.
   */
  final def getBit[K: Schema](key: K, offset: Long): IO[RedisError, Long] = _getBit[K].run((key, offset))

  /**
   * Get a substring of the string stored at key.
   *
   * @param key
   *   Key of the string to get a substring of
   * @param range
   *   Range of the substring
   * @return
   *   Returns the substring.
   */
  final def getRange[K: Schema](key: K, range: Range): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getRange[K, R].run((key, range))
    }

  /**
   * Set the string value of a key and return its old value.
   *
   * @param key
   *   Key of string to set
   * @param value
   *   New value of the string
   * @return
   *   Returns the previous value of the string or None if it did not previously have a value.
   */
  final def getSet[K: Schema, V: Schema](key: K, value: V): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getSet[K, V, R].run((key, value))
    }

  /**
   * Get the string value of a key and delete it on success (if and only if the key's value type is a string).
   *
   * @param key
   *   Key to get the value of
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getDel[K: Schema](key: K): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getDel[K, R].run(key)
    }

  /**
   * Get the value of key and set its expiration.
   *
   * @param key
   *   Key to get the value of
   * @param expire
   *   The option which can modify command behavior. e.g. use `Expire.SetExpireSeconds` set the specified expire time in
   *   seconds
   * @param expireTime
   *   Time in seconds/milliseconds until the string should expire
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, expire: Expire, expireTime: Duration): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getEx[K, R].run((key, expire, expireTime))
    }

  /**
   * Get the value of key and set its expiration.
   *
   * @param key
   *   Key to get the value of
   * @param expiredAt
   *   The option which can modify command behavior. e.g. use `Expire.SetExpireAtSeconds` set the specified Unix time at
   *   which the key will expire in seconds
   * @param timestamp
   *   an absolute Unix timestamp (seconds/milliseconds since January 1, 1970)
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, expiredAt: ExpiredAt, timestamp: Instant): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getExAt[K, R].run((key, expiredAt, timestamp))
    }

  /**
   * Get the value of key and remove the time to live associated with the key.
   *
   * @param key
   *   Key to get the value of
   * @param persist
   *   if true, remove the time to live associated with the key, otherwise not
   * @return
   *   Returns the value of the string or None if it did not previously have a value.
   */
  final def getEx[K: Schema](key: K, persist: Boolean): ResultBuilder1[Option] =
    new ResultBuilder1[Option] {
      def returning[R: Schema]: IO[RedisError, Option[R]] = _getExDel[K, R].run((key, persist))
    }

  /**
   * Increment the integer value of a key by one.
   *
   * @param key
   *   Key of the string to increment
   * @return
   *   Returns the value of key after the increment.
   */
  final def incr[K: Schema](key: K): IO[RedisError, Long] = _incr[K].run(key)

  /**
   * Increment the integer value of a key by the given amount.
   *
   * @param key
   *   Key of the value to increment
   * @param increment
   *   Amount to increment the value by
   * @return
   *   Returns the value of key after the increment.
   */
  final def incrBy[K: Schema](key: K, increment: Long): IO[RedisError, Long] = _incrBy[K].run((key, increment))

  /**
   * Increment the float value of a key by the given amount.
   *
   * @param key
   *   Key of the value to increment
   * @param increment
   *   Amount to increment the value by
   * @return
   *   Returns the value of key after the increment.
   */
  final def incrByFloat[K: Schema](key: K, increment: Double): IO[RedisError, Double] =
    _incrByFloat[K].run((key, increment))

  /**
   * Get all the values of the given keys.
   *
   * @param key
   *   First key to get
   * @param keys
   *   Subsequent keys to get
   * @return
   *   Returns the values of the given keys.
   */
  final def mGet[K: Schema](key: K, keys: K*): ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda] =
    new ResultBuilder1[({ type lambda[x] = Chunk[Option[x]] })#lambda] {
      def returning[V: Schema]: IO[RedisError, Chunk[Option[V]]] = _mGet[K, V].run((key, keys.toList))
    }

  /**
   * Set multiple keys to multiple values.
   *
   * @param keyValue
   *   Tuple of key and value, first one to set
   * @param keyValues
   *   Subsequent tuples of key values
   */
  final def mSet[K: Schema, V: Schema](keyValue: (K, V), keyValues: (K, V)*): IO[RedisError, Unit] =
    _mSet[K, V].run((keyValue, keyValues.toList))

  /**
   * Set multiple keys to multiple values only if none of the keys exist.
   *
   * @param keyValue
   *   First key value to set
   * @param keyValues
   *   Subsequent key values to set
   * @return
   *   1 if the all the keys were set. 0 if no key was set (at least one key already existed).
   */
  final def mSetNx[K: Schema, V: Schema](keyValue: (K, V), keyValues: (K, V)*): IO[RedisError, Boolean] =
    _mSetNx[K, V].run((keyValue, keyValues.toList))

  /**
   * Set the value and expiration in milliseconds of a key.
   *
   * @param key
   *   Key of the string to set the expiry time on
   * @param milliseconds
   *   Time in milliseconds until the string should expire
   * @param value
   *   Value to set
   */
  final def pSetEx[K: Schema, V: Schema](key: K, milliseconds: Duration, value: V): IO[RedisError, Unit] =
    _pSetEx[K, V].run((key, milliseconds, value))

  /**
   * Set the string value of a key.
   *
   * @param key
   *   Key of the string to set
   * @param value
   *   Value to set
   * @param expireTime
   *   Time until the string expires
   * @param update
   *   Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which nly sets the key if
   *   it does not exist
   * @param keepTtl
   *   When set any previously set expire time remains unchanged
   * @return
   *   true if set was executed correctly, false otherwise.
   */
  final def set[K: Schema, V: Schema](
    key: K,
    value: V,
    expireTime: Option[Duration] = None,
    update: Option[Update] = None,
    keepTtl: Option[KeepTtl] = None
  ): IO[RedisError, Boolean] = _set[K, V].run((key, value, expireTime, update, keepTtl))

  /**
   * Sets or clears the bit at offset in the string value stored at key.
   *
   * @param key
   *   Key of the string to set or clear bits
   * @param offset
   *   Offset at which to set or clear the bit
   * @param value
   *   True if bit should be set, False if it should be cleared
   * @return
   *   Returns the original bit value stored at offset.
   */
  final def setBit[K: Schema](key: K, offset: Long, value: Boolean): IO[RedisError, Boolean] =
    _setBit[K].run((key, offset, value))

  /**
   * Set the value and expiration of a key.
   *
   * @param key
   *   Key of the value to update
   * @param expiration
   *   Expiration time for the value
   * @param value
   *   New value to set
   */
  final def setEx[K: Schema, V: Schema](key: K, expiration: Duration, value: V): IO[RedisError, Unit] =
    _setEx[K, V].run((key, expiration, value))

  /**
   * Set the value of a key, only if the key does not exist.
   *
   * @param key
   *   Key of the value to set if the key does not exist
   * @param value
   *   Value to set
   * @return
   *   Returns 1 if the key was set. 0 if the key was not set.
   */
  final def setNx[K: Schema, V: Schema](key: K, value: V): IO[RedisError, Boolean] = _setNx[K, V].run((key, value))

  /**
   * Overwrite part of a string at key starting at the specified offset.
   *
   * @param key
   *   Key of the string to overwite
   * @param offset
   *   Offset to start writing
   * @param value
   *   Value to overwrite with
   * @return
   *   Returns the length of the string after it was modified by the command.
   */
  final def setRange[K: Schema, V: Schema](key: K, offset: Long, value: V): IO[RedisError, Long] =
    _setRange[K, V].run((key, offset, value))

  /**
   * Get the length of a value stored in a key.
   *
   * @param key
   *   Key of the string to get the length of
   * @return
   *   Returns the length of the string.
   */
  final def strLen[K: Schema](key: K): IO[RedisError, Long] = _strLen[K].run(key)

  /**
   * Get the longest common subsequence of values stored in the given keys.
   *
   * @param command
   *   type of value it (possible values are Strings and Keys)
   * @param keyA
   *   first value that will contain subsequence
   * @param keyB
   *   second value that will contain subsequence
   * @param lcsQueryType
   *   modifier that will affect the output
   * @return
   *   Without modifiers returns the string representing the longest common substring. When LEN is given the command
   *   returns the length of the longest common substring. When IDX is given the command returns an array with the LCS
   *   length and all the ranges in both the strings, start and end offset for each string, where there are matches.
   *   When withMatchLen is given each array representing a match will also have the length of the match (see examples).
   */
  final def strAlgoLcs[K: Schema](
    command: StrAlgoLCS,
    keyA: K,
    keyB: K,
    lcsQueryType: Option[StrAlgoLcsQueryType] = None
  ): IO[RedisError, LcsOutput] = _strAlgoLcs[K].run((command.stringify, keyA, keyB, lcsQueryType))
}
