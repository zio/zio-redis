package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Strings {
  import Strings._

  // Append a value to a key
  final def append(key: String, value: String): ZIO[RedisExecutor, RedisError, Long] = Append.run((key, value))

  // Count set bits in a string
  final def bitCount(key: String, range: Option[Range] = None): ZIO[RedisExecutor, RedisError, Long] =
    BitCount.run((key, range))

  // Perform arbitrary bitfield integer operations on strings
  final def bitField(
    key: String,
    firstCommand: BitFieldCommand,
    restCommands: BitFieldCommand*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[Long]]] = BitField.run((key, (firstCommand, restCommands.toList)))

  // Perform bitwise operations between strings
  final def bitOp(
    operation: BitOperation,
    destKey: String,
    firstSrcKey: String,
    restSrcKeys: String*
  ): ZIO[RedisExecutor, RedisError, Long] =
    BitOp.run((operation, destKey, (firstSrcKey, restSrcKeys.toList)))

  // Find first bit set or clear in a string
  final def bitPos(key: String, bit: Boolean, range: Option[BitPosRange] = None): ZIO[RedisExecutor, RedisError, Long] =
    BitPos.run((key, bit, range))

  // Decrement the integer value of a key by one
  final def decr(key: String): ZIO[RedisExecutor, RedisError, Long] = Decr.run(key)

  // Decrement the integer value of a key by the given number
  final def decrBy(key: String, decrement: Long): ZIO[RedisExecutor, RedisError, Long] = DecrBy.run((key, decrement))

  // Get the value of a key
  final def get(key: String): ZIO[RedisExecutor, RedisError, Option[String]] = Get.run(key)

  // Returns the bit value at offset in the string value stored at key
  final def getBit(key: String, offset: Long): ZIO[RedisExecutor, RedisError, Long] = GetBit.run((key, offset))

  // Get a substring of the string stored at key
  final def getRange(key: String, range: Range): ZIO[RedisExecutor, RedisError, String] = GetRange.run((key, range))

  // Set the string value of a key and return its old value
  final def getSet(key: String, value: String): ZIO[RedisExecutor, RedisError, Option[String]] =
    GetSet.run((key, value))

  // Increment the integer value of a key by one
  final def incr(key: String): ZIO[RedisExecutor, RedisError, Long] = Incr.run(key)

  // Increment the integer value of a key by the given amount
  final def incrBy(key: String, increment: Long): ZIO[RedisExecutor, RedisError, Long] = IncrBy.run((key, increment))

  // Increment the float value of a key by the given amount
  final def incrByFloat(key: String, increment: Double): ZIO[RedisExecutor, RedisError, String] =
    IncrByFloat.run((key, increment))

  // Get all the values of the given keys
  final def mGet(firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
    MGet.run((firstKey, restKeys.toList))

  // Set multiple keys to multiple values
  final def mSet(
    firstKeyValue: (String, String),
    restKeyValues: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Unit] =
    MSet.run((firstKeyValue, restKeyValues.toList))

  // Set multiple keys to multiple values only if none of the keys exist
  final def mSetNx(
    firstKeyValue: (String, String),
    restKeyValues: (String, String)*
  ): ZIO[RedisExecutor, RedisError, Boolean] =
    MSetNx.run((firstKeyValue, restKeyValues.toList))

  // Set the value and expiration in milliseconds of a key
  final def pSetEx(key: String, milliseconds: Duration, value: String): ZIO[RedisExecutor, RedisError, Unit] =
    PSetEx.run((key, milliseconds, value))

  /** Set the string value of a key
   *
   * You can optionally set the expireTime, the update method and the KeepTTL parameter
   * Update can be Update.SetExisting which only sets the key if it exists, or Update.SetNew which
   * only sets the key if it does not exist.
   * If you specify KEEPTTL then any previously set expire time remains unchanged.
   */
  final def set(
    key: String,
    value: String,
    expireTime: Option[Duration] = None,
    update: Option[Update] = None,
    keepTtl: Option[KeepTtl] = None
  ): ZIO[RedisExecutor, RedisError, Option[Unit]] = Set.run((key, value, expireTime, update, keepTtl))

  // Sets or clears the bit at offset in the string value stored at key
  final def setBit(key: String, offset: Long, value: Boolean): ZIO[RedisExecutor, RedisError, Boolean] =
    SetBit.run((key, offset, value))

  // Set the value and expiration of a key
  final def setEx(key: String, expiration: Duration, value: String): ZIO[RedisExecutor, RedisError, Unit] =
    SetEx.run((key, expiration, value))

  // Set the value of a key, only if the key does not exist
  final def setNx(key: String, value: String): ZIO[RedisExecutor, RedisError, Boolean] = SetNx.run((key, value))

  // Overwrite part of a string at key starting at the specified offset
  final def setRange(key: String, offset: Long, value: String): ZIO[RedisExecutor, RedisError, Long] =
    SetRange.run((key, offset, value))

  // Get the length of a value stored in a key
  final def strLen(key: String): ZIO[RedisExecutor, RedisError, Long] = StrLen.run(key)
}

private object Strings {
  final val Append   = RedisCommand("APPEND", Tuple2(StringInput, StringInput), LongOutput)
  final val BitCount = RedisCommand("BITCOUNT", Tuple2(StringInput, OptionalInput(RangeInput)), LongOutput)

  final val BitField =
    RedisCommand(
      "BITFIELD",
      Tuple2(StringInput, NonEmptyList(BitFieldCommandInput)),
      ChunkOptionalLongOutput
    )

  final val BitOp =
    RedisCommand("BITOP", Tuple3(BitOperationInput, StringInput, NonEmptyList(StringInput)), LongOutput)

  final val BitPos =
    RedisCommand("BITPOS", Tuple3(StringInput, BoolInput, OptionalInput(BitPosRangeInput)), LongOutput)

  final val Decr        = RedisCommand("DECR", StringInput, LongOutput)
  final val DecrBy      = RedisCommand("DECRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val Get         = RedisCommand("GET", StringInput, OptionalOutput(MultiStringOutput))
  final val GetBit      = RedisCommand("GETBIT", Tuple2(StringInput, LongInput), LongOutput)
  final val GetRange    = RedisCommand("GETRANGE", Tuple2(StringInput, RangeInput), MultiStringOutput)
  final val GetSet      = RedisCommand("GETSET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val Incr        = RedisCommand("INCR", StringInput, LongOutput)
  final val IncrBy      = RedisCommand("INCRBY", Tuple2(StringInput, LongInput), LongOutput)
  final val IncrByFloat = RedisCommand("INCRBYFLOAT", Tuple2(StringInput, DoubleInput), MultiStringOutput)
  final val MGet        = RedisCommand("MGET", NonEmptyList(StringInput), ChunkOptionalMultiStringOutput)
  final val MSet        = RedisCommand("MSET", NonEmptyList(Tuple2(StringInput, StringInput)), UnitOutput)
  final val MSetNx      = RedisCommand("MSETNX", NonEmptyList(Tuple2(StringInput, StringInput)), BoolOutput)
  final val PSetEx      = RedisCommand("PSETEX", Tuple3(StringInput, DurationMillisecondsInput, StringInput), UnitOutput)

  final val Set =
    RedisCommand(
      "SET",
      Tuple5(
        StringInput,
        StringInput,
        OptionalInput(DurationTtlInput),
        OptionalInput(UpdateInput),
        OptionalInput(KeepTtlInput)
      ),
      OptionalOutput(UnitOutput)
    )

  final val SetBit   = RedisCommand("SETBIT", Tuple3(StringInput, LongInput, BoolInput), BoolOutput)
  final val SetEx    = RedisCommand("SETEX", Tuple3(StringInput, DurationSecondsInput, StringInput), UnitOutput)
  final val SetNx    = RedisCommand("SETNX", Tuple2(StringInput, StringInput), BoolOutput)
  final val SetRange = RedisCommand("SETRANGE", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val StrLen   = RedisCommand("STRLEN", StringInput, LongOutput)
}
