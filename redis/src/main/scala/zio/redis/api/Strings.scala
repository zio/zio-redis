package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Strings {
  import Strings._

  final def append(a: String, b: String): ZIO[RedisExecutor, RedisError, Long] = Append.run((a, b))

  final def bitCount(a: String, b: Option[Range] = None): ZIO[RedisExecutor, RedisError, Long] = BitCount.run((a, b))

  final def bitField(
    a: String,
    b: BitFieldCommand,
    bs: BitFieldCommand*
  ): ZIO[RedisExecutor, RedisError, Chunk[Option[Long]]] = BitField.run((a, (b, bs.toList)))

  final def bitOp(a: BitOperation, b: String, c: String, cs: String*): ZIO[RedisExecutor, RedisError, Long] =
    BitOp.run((a, b, (c, cs.toList)))

  final def bitPos(a: String, b: Boolean, c: Option[BitPosRange] = None): ZIO[RedisExecutor, RedisError, Long] =
    BitPos.run((a, b, c))

  final def decr(a: String): ZIO[RedisExecutor, RedisError, Long] = Decr.run(a)

  final def decrBy(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = DecrBy.run((a, b))

  final def get(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = Get.run(a)

  final def getBit(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = GetBit.run((a, b))

  final def getRange(a: String, b: Range): ZIO[RedisExecutor, RedisError, String] = GetRange.run((a, b))

  final def getSet(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = GetSet.run((a, b))

  final def incr(a: String): ZIO[RedisExecutor, RedisError, Long] = Incr.run(a)

  final def incrBy(a: String, b: Long): ZIO[RedisExecutor, RedisError, Long] = IncrBy.run((a, b))

  final def incrByFloat(a: String, b: Double): ZIO[RedisExecutor, RedisError, String] = IncrByFloat.run((a, b))

  final def mGet(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[Option[String]]] =
    MGet.run((a, as.toList))

  final def mSet(a: (String, String), as: (String, String)*): ZIO[RedisExecutor, RedisError, Unit] =
    MSet.run((a, as.toList))

  final def mSetNx(a: (String, String), as: (String, String)*): ZIO[RedisExecutor, RedisError, Boolean] =
    MSetNx.run((a, as.toList))

  final def pSetEx(a: String, b: Duration, c: String): ZIO[RedisExecutor, RedisError, Unit] = PSetEx.run((a, b, c))

  final def set(
    a: String,
    b: String,
    c: Option[Duration] = None,
    d: Option[Update] = None,
    e: Option[KeepTtl] = None
  ): ZIO[RedisExecutor, RedisError, Option[Unit]] = Set.run((a, b, c, d, e))

  final def setBit(a: String, b: Long, c: Boolean): ZIO[RedisExecutor, RedisError, Boolean] = SetBit.run((a, b, c))

  final def setEx(a: String, b: Duration, c: String): ZIO[RedisExecutor, RedisError, Unit] = SetEx.run((a, b, c))

  final def setNx(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = SetNx.run((a, b))

  final def setRange(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Long] = SetRange.run((a, b, c))

  final def strLen(a: String): ZIO[RedisExecutor, RedisError, Long] = StrLen.run(a)
}

private[api] object Strings {
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
