package zio.redis.api

import java.time.Duration

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Lists {
  import Lists._

  final def brPopLPush(a: String, b: String, c: Duration): ZIO[RedisExecutor, RedisError, Option[String]] =
    BrPopLPush.run((a, b, c))

  final def lIndex(a: String, b: Long): ZIO[RedisExecutor, RedisError, Option[String]] = LIndex.run((a, b))

  final def lLen(a: String): ZIO[RedisExecutor, RedisError, Long] = LLen.run(a)

  final def lPop(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = LPop.run(a)

  final def lPush(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPush.run((a, (b, bs.toList)))

  final def lPushX(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    LPushX.run((a, (b, bs.toList)))

  final def lRange(a: String, b: Range): ZIO[RedisExecutor, RedisError, Chunk[String]] = LRange.run((a, b))

  final def lRem(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Long] = LRem.run((a, b, c))

  final def lSet(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Unit] = LSet.run((a, b, c))

  final def lTrim(a: String, b: Range): ZIO[RedisExecutor, RedisError, Unit] = LTrim.run((a, b))

  final def rPop(a: String): ZIO[RedisExecutor, RedisError, Option[String]] = RPop.run(a)

  final def rPopLPush(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = RPopLPush.run((a, b))

  final def rPush(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPush.run((a, (b, bs.toList)))

  final def rPushX(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    RPushX.run((a, (b, bs.toList)))

  final def blPop(a: String, as: String*)(b: Duration): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
    BlPop.run(((a, as.toList), b))

  final def brPop(a: String, as: String*)(b: Duration): ZIO[RedisExecutor, RedisError, Option[(String, String)]] =
    BrPop.run(((a, as.toList), b))

  final def lInsert(a: String, b: Position, c: String, d: String): ZIO[RedisExecutor, RedisError, Long] =
    LInsert.run((a, b, c, d))
}

private[redis] object Lists {
  final val BrPopLPush =
    RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val LIndex = RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput))
  final val LLen   = RedisCommand("LLEN", StringInput, LongOutput)
  final val LPop   = RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput))
  final val LPush  = RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LPushX = RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LRange = RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)
  final val LRem   = RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val LSet   = RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput)
  final val LTrim  = RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val RPop   = RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput))

  final val RPopLPush =
    RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))

  final val RPush  = RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val RPushX = RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val BlPop  = RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)
  final val BrPop  = RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)

  final val LInsert =
    RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput)
}
