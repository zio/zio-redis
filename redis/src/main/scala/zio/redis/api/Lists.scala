package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import java.time.Duration

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

private[api] object Lists {
  final val BrPopLPush =
    new RedisCommand(
      "BRPOPLPUSH",
      Tuple3(StringInput, StringInput, DurationSecondsInput),
      OptionalOutput(MultiStringOutput)
    )

  final val LIndex = new RedisCommand("LINDEX", Tuple2(StringInput, LongInput), OptionalOutput(MultiStringOutput))
  final val LLen   = new RedisCommand("LLEN", StringInput, LongOutput)
  final val LPop   = new RedisCommand("LPOP", StringInput, OptionalOutput(MultiStringOutput))
  final val LPush  = new RedisCommand("LPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LPushX = new RedisCommand("LPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val LRange = new RedisCommand("LRANGE", Tuple2(StringInput, RangeInput), ChunkOutput)
  final val LRem   = new RedisCommand("LREM", Tuple3(StringInput, LongInput, StringInput), LongOutput)
  final val LSet   = new RedisCommand("LSET", Tuple3(StringInput, LongInput, StringInput), UnitOutput)
  final val LTrim  = new RedisCommand("LTRIM", Tuple2(StringInput, RangeInput), UnitOutput)
  final val RPop   = new RedisCommand("RPOP", StringInput, OptionalOutput(MultiStringOutput))

  final val RPopLPush =
    new RedisCommand("RPOPLPUSH", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))

  final val RPush  = new RedisCommand("RPUSH", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val RPushX = new RedisCommand("RPUSHX", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val BlPop  = new RedisCommand("BLPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)
  final val BrPop  = new RedisCommand("BRPOP", Tuple2(NonEmptyList(StringInput), DurationSecondsInput), KeyElemOutput)

  final val LInsert =
    new RedisCommand("LINSERT", Tuple4(StringInput, PositionInput, StringInput, StringInput), LongOutput)
}
