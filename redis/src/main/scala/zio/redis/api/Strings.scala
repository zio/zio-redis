package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Strings {
  import Strings._

  /**
   * Append a value to a key
   *
   * @param key Key of the string to add the value to
   * @param value Value to append to the string
   * @return Returns the length of the string after the append operation
   */
  final def append(key: String, value: String): ZIO[RedisExecutor, RedisError, Long] = Append.run((key, value))

  /**
   * Count set bits in a string
   *
   * @param key Key of the string of which to count the bits
   * @param range Range of bytes to count
   * @return Returns the number of bits set to 1
   */
  final def bitCount(key: String, range: Option[Range] = None): ZIO[RedisExecutor, RedisError, Long] =
    BitCount.run((key, range))

  /**
   * Perform arbitrary bitfield integer operations on strings
   *
   * @param key Key of the string to operate on
   * @param command First command to apply
   * @param commands Subsequent commands to apply
   * @return Returns an optional long result of each command applied
   */
  final def bitField(
    key: String,
    command: BitFieldCommand,
    commands: BitFieldCommand*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[Long]]] = BitField.run((key, (command, commands.toList)))

  /**
   * Perform bitwise operations between strings
   *
   * @param operation Bit operation to apply
   * @param destKey Key of destination string to store the result
   * @param srcKey First source key to apply the operation to
   * @param srcKeys Subsequent source keys to apply the operation to
   * @return Returns size of the string stored in the destination key, that is equal to the size of the longest input string
   */
  final def bitOp(
    operation: BitOperation,
    destKey: String,
    srcKey: String,
    srcKeys: String*
  ): ZIO[RedisExecutor, RedisError, Long] =
    BitOp.run((operation, destKey, (srcKey, srcKeys.toList)))

  /**
   * Find first bit set or clear in a string
   *
   * @param key Key of the string to search within
   * @param bit Whether to search for a set bit or a cleared bit
   * @param range Range of bytes to search
   * @return Returns the position of the first bit set to 1 or 0 according to the request
   */
  final def bitPos(key: String, bit: Boolean, range: Option[BitPosRange] = None): ZIO[RedisExecutor, RedisError, Long] =
    BitPos.run((key, bit, range))

  /**
   * Decrement the integer value of a key by one
   *
   * @param key Key to decrement
   * @return Returns the value of key after the decrement
   */
  final def decr(key: String): ZIO[RedisExecutor, RedisError, Long] = Decr.run(key)

  /**
   * Decrement the integer value of a key by the given number
   *
   * @param key Key of the integer value to decrement
   * @param decrement Amount to decrement by
   * @return Returns the value of key after the decrement
   */
  final def decrBy(key: String, decrement: Long): ZIO[RedisExecutor, RedisError, Long] = DecrBy.run((key, decrement))

  /**
   * Get the value of a key
   *
   * @param key Key to get the value of
   * @return Returns the value of the string or None if it does not exist
   */
  final def get(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = Get.run(key)

  /**
   * Returns the bit value at offset in the string value stored at key
   *
   * @param key Key of the string to get the bit from
   * @param offset Offset to the bit
   * @return Returns the bit value stored at offset
   */
  final def getBit(key: String, offset: Long): ZIO[RedisExecutor, RedisError, Long] = GetBit.run((key, offset))

  /**
   * Get a substring of the string stored at key
   *
   * @param key Key of the string to get a substring of
   * @param range Range of the substring
   * @return Returns the substring
   */
  final def getRange(key: String, range: Range): ZIO[RedisExecutor, RedisError, String] = GetRange.run((key, range))

  /**
   * Set the string value of a key and return its old value
   *
   * @param key Key of string to set
   * @param value New value of the string
   * @return Returns the previous value of the string or None if it did not previously have a value
   */
  final def getSet(key: String, value: String): ZIO[RedisExecutor, RedisError, Option[String]] =
    GetSet.run((key, value))

  /**
   * Increment the integer value of a key by one
   *
   * @param key Key of the string to increment
   * @return Returns the value of key after the increment
   */
  final def incr(key: String): ZIO[RedisExecutor, RedisError, Long] = Incr.run(key)

  /**
   * Increment the integer value of a key by the given amount
   *
   * @param key Key of the value to increment
   * @param increment Amount to increment the value by
   * @return Returns the value of key after the increment
   */
  final def incrBy(key: String, increment: Long): ZIO[RedisExecutor, RedisError, Long] = IncrBy.run((key, increment))

  /**
   * Increment the float value of a key by the given amount
   *
   * @param key Key of the value to increment
   * @param increment Amount to increment the value by
   * @return Returns the value of key after the increment
   */
  final def incrByFloat(key: String, increment: Double): ZIO[RedisExecutor, RedisError, String] =
    IncrByFloat.run((key, increment))

  /**
   * Get all the values of the given keys
   *
   * @param key First key to get
   * @param keys Subsequent keys to get
   * @return Returns the values of the given keys
   */
  final def mGet(key: String, keys: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
    MGet.run((key, keys.toList))

  /**
   * Set multiple keys to multiple values
   *
   * @param keyValue Tuple of key and value, first one to set
   * @param keyValues Subsequent tuples of key values
   */
  final def mSet(
    keyValue: (String, String),
    keyValues: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Unit] =
    MSet.run((keyValue, keyValues.toList))

  /**
   * Set multiple keys to multiple values only if none of the keys exist
   *
   * @param keyValue First key value to set
   * @param keyValues Subsequent key values to set
   * @return 1 if the all the keys were set. 0 if no key was set (at least one key already existed)
   */
  final def mSetNx(
    keyValue: (String, String),
    keyValues: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Boolean] =
    MSetNx.run((keyValue, keyValues.toList))

  /**
   * Set the value and expiration in milliseconds of a key
   *
   * @param key Key of the string to set the expiry time on
   * @param milliseconds Time in milliseconds until the string should expire
   * @param value Value to set
   */
  final def pSetEx(key: String, milliseconds: Duration, value: String): ZIO[RedisExecutor, RedisError, Unit] =
    PSetEx.run((key, milliseconds, value))

  /**
   * Set the string value of a key
   *
   * @param key Key of the string to set
   * @param value Value to set
   * @param expireTime Time until the string expires
   * @param update Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which nly sets the key if it does not exist
   * @param keepTtl When set any previously set expire time remains unchanged
   */
  final def set(
    key: String,
    value: String,
    expireTime: Option[Duration] = None,
    update: Option[Update] = None,
    keepTtl: Option[KeepTtl] = None
  ): ZIO[RedisExecutor, RedisError, Boolean] = Set.run((key, value, expireTime, update, keepTtl))

  /**
   * Sets or clears the bit at offset in the string value stored at key
   *
   * @param key Key of the string to set or clear bits
   * @param offset Offset at which to set or clear the bit
   * @param value True if bit should be set, False if it should be cleared
   * @return Returns the original bit value stored at offset
   */
  final def setBit(key: String, offset: Long, value: Boolean): ZIO[RedisExecutor, RedisError, Boolean] =
    SetBit.run((key, offset, value))

  /**
   * Set the value and expiration of a key
   *
   * @param key Key of the value to update
   * @param expiration Expiration time for the value
   * @param value New value to set
   */
  final def setEx(key: String, expiration: Duration, value: String): ZIO[RedisExecutor, RedisError, Unit] =
    SetEx.run((key, expiration, value))

  /**
   * Set the value of a key, only if the key does not exist
   *
   * @param key Key of the value to set if the key does not exist
   * @param value Value to set
   * @return Returns 1 if the key was set. 0 if the key was not set
   */
  final def setNx(key: String, value: String): ZIO[RedisExecutor, RedisError, Boolean] = SetNx.run((key, value))

  /**
   * Overwrite part of a string at key starting at the specified offset
   *
   * @param key Key of the string to overwite
   * @param offset Offset to start writing
   * @param value Value to overwrite with
   * @return Returns the length of the string after it was modified by the command
   */
  final def setRange(key: String, offset: Long, value: String): ZIO[RedisExecutor, RedisError, Long] =
    SetRange.run((key, offset, value))

  /**
   * Get the length of a value stored in a key
   *
   * @param key Key of the string to get the length of
   * @return Returns the length of the string
   */
  final def strLen(key: String): ZIO[RedisExecutor, RedisError, Long] = StrLen.run(key)
}

private[redis] object Strings {
  final val Append: RedisCommand[(String, String), Long]          =
    RedisCommand("APPEND", Tuple2(StringInput, StringInput), LongOutput)
  final val BitCount: RedisCommand[(String, Option[Range]), Long] =
    RedisCommand("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)

  final val BitField: RedisCommand[(String, (BitFieldCommand, List[BitFieldCommand])), Chunk[Option[Long]]] =
    RedisCommand(
      "BITFIELD",
      Tuple2(StringInput, NonEmptyList(BitFieldCommandInput)),
      ChunkOptionalLongOutput
    )

  final val BitOp: RedisCommand[(BitOperation, String, (String, List[String])), Long] =
    RedisCommand("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val BitPos: RedisCommand[(String, Boolean, Option[BitPosRange]), Long] =
    RedisCommand("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)

  final val Decr: RedisCommand[String, Long]                                          = RedisCommand("DECR", StringInput, LongOutput)
  final val DecrBy: RedisCommand[(String, Long), Long]                                =
    RedisCommand("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val Get: RedisCommand[String, Option[String]]                                 =
    RedisCommand("GET", StringInput, OptionalOutput(MultiStringOutput))
  final val GetBit: RedisCommand[(String, Long), Long]                                =
    RedisCommand("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  final val GetRange: RedisCommand[(String, Range), String]                           =
    RedisCommand("GETRANGE", Tuple2(StringInput, RangeInput), MultiStringOutput)
  final val GetSet: RedisCommand[(String, String), Option[String]]                    =
    RedisCommand("GETSET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val Incr: RedisCommand[String, Long]                                          = RedisCommand("INCR", StringInput, LongOutput)
  final val IncrBy: RedisCommand[(String, Long), Long]                                =
    RedisCommand("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val IncrByFloat: RedisCommand[(String, Double), String]                       =
    RedisCommand("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), MultiStringOutput)
  final val MGet: RedisCommand[(String, List[String]), Chunk[Option[String]]]         =
    RedisCommand("MGET", NonEmptyList(StringInput), ChunkOptionalMultiStringOutput)
  final val MSet: RedisCommand[((String, String), List[(String, String)]), Unit]      =
    RedisCommand("MSET", NonEmptyList(Tuple2(StringInput, StringInput)), UnitOutput)
  final val MSetNx: RedisCommand[((String, String), List[(String, String)]), Boolean] =
    RedisCommand("MSETNX", NonEmptyList(Tuple2(StringInput, StringInput)), BoolOutput)
  final val PSetEx: RedisCommand[(String, Duration, String), Unit]                    =
    RedisCommand("PSETEX", Tuple3(StringInput, DurationMillisecondsInput, StringInput), UnitOutput)

  final val Set: RedisCommand[(String, String, Option[Duration], Option[Update], Option[KeepTtl]), Boolean] =
    RedisCommand(
      "SET",
      Tuple5(
        StringInput,
        StringInput,
        OptionalInput(DurationTtlInput),
        OptionalInput(UpdateInput),
        OptionalInput(KeepTtlInput)
      ),
      SetOutput
    )

  final val SetBit: RedisCommand[(String, Long, Boolean), Boolean] =
    RedisCommand("SETBIT", Tuple3(StringInput, LongInput, BoolInput), BoolOutput)
  final val SetEx: RedisCommand[(String, Duration, String), Unit]  =
    RedisCommand("SETEX", Tuple3(StringInput, DurationSecondsInput, StringInput), UnitOutput)
  final val SetNx: RedisCommand[(String, String), Boolean]         =
    RedisCommand("SETNX", Tuple2(StringInput, StringInput), BoolOutput)
  final val SetRange: RedisCommand[(String, Long, String), Long]   =
    RedisCommand("SETRANGE", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val StrLen: RedisCommand[String, Long]                     = RedisCommand("STRLEN", StringInput, LongOutput)
}
