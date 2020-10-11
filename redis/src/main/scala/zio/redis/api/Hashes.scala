package zio.redis.api

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import scala.util.matching.Regex

trait Hashes {
  import Hashes._

  final def hDel(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    HDel.run((a, (b, bs.toList)))

  final def hExists(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = HExists.run((a, b))

  final def hGet(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[String]] = HGet.run((a, b))

  final def hGetAll(a: String): ZIO[RedisExecutor, RedisError, Map[String, String]] = HGetAll.run(a)

  final def hIncrBy(a: String, b: String, c: Long): ZIO[RedisExecutor, RedisError, Long] = HIncrBy.run((a, b, c))

  final def hIncrByFloat(a: String, b: String, c: Double): ZIO[RedisExecutor, RedisError, Double] =
    HIncrByFloat.run((a, b, c))

  final def hKeys(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = HKeys.run(a)

  final def hLen(a: String): ZIO[RedisExecutor, RedisError, Long] = HLen.run(a)

  final def hmGet(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    HmGet.run((a, (b, bs.toList)))

  final def hScan(
    a: Long,
    b: Option[Regex] = None,
    c: Option[Long] = None,
    d: Option[String] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = HScan.run((a, b, c, d))

  final def hSet(a: String, b: (String, String), bs: (String, String)*): ZIO[RedisExecutor, RedisError, Long] =
    HSet.run((a, (b, bs.toList)))

  final def hSetNx(a: String, b: String, c: String): ZIO[RedisExecutor, RedisError, Boolean] = 
    HSetNx.run((a, b, c))

  final def hStrLen(a: String, b: String): ZIO[RedisExecutor, RedisError, Long] = HStrLen.run((a, b))

  final def hVals(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = HVals.run(a)
}

private[api] object Hashes {
  final val HDel    = new RedisCommand("HDEL", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val HExists = new RedisCommand("HEXISTS", Tuple2(StringInput, StringInput), BoolOutput)
  final val HGet    = new RedisCommand("HGET", Tuple2(StringInput, StringInput), OptionalOutput(MultiStringOutput))
  final val HGetAll = new RedisCommand("HGETALL", StringInput, KeyValueOutput)
  final val HIncrBy = new RedisCommand("HINCRBY", Tuple3(StringInput, StringInput, LongInput), LongOutput)

  final val HIncrByFloat =
    new RedisCommand("HINCRBYFLOAT", Tuple3(StringInput, StringInput, DoubleInput), IncrementOutput)

  final val HKeys = new RedisCommand("HKEYS", StringInput, ChunkOutput)
  final val HLen  = new RedisCommand("HLEN", StringInput, LongOutput)
  final val HmGet = new RedisCommand("HMGET", Tuple2(StringInput, NonEmptyList(StringInput)), ChunkOutput)

  final val HScan =
    new RedisCommand(
      "HSCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val HSet =
    new RedisCommand("HSET", Tuple2(StringInput, NonEmptyList(Tuple2(StringInput, StringInput))), LongOutput)

  final val HSetNx  = new RedisCommand("HSETNX", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val HStrLen = new RedisCommand("HSTRLEN", Tuple2(StringInput, StringInput), LongOutput)
  final val HVals   = new RedisCommand("HVALS", StringInput, ChunkOutput)
}
