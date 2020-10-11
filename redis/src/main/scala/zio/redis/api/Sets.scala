package zio.redis.api

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Sets {
  import Sets._

  final def sAdd(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    SAdd.run((a, (b, bs.toList)))

  final def sCard(a: String): ZIO[RedisExecutor, RedisError, Long] = SCard.run(a)

  final def sDiff(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = SDiff.run((a, as.toList))

  final def sDiffStore(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    SDiffStore.run((a, (b, bs.toList)))

  final def sInter(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = SInter.run((a, as.toList))

  final def sInterStore(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    SInterStore.run((a, (b, bs.toList)))

  final def sIsMember(a: String, b: String): ZIO[RedisExecutor, RedisError, Boolean] = SIsMember.run((a, b))

  final def sMembers(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = SMembers.run(a)

  final def sMove(a: String, b: String, c: String): ZIO[RedisExecutor, RedisError, Boolean] = SMove.run((a, b, c))

  final def sPop(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = SPop.run((a, b))

  final def sRandMember(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SRandMember.run((a, b))

  final def sRem(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    SRem.run((a, (b, bs.toList)))

  final def sScan(
    a: String,
    b: Long,
    c: Option[Regex] = None,
    d: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = SScan.run((a, b, c, d))

  final def sUnion(a: String, as: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] = SUnion.run((a, as.toList))

  final def sUnionStore(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    SUnionStore.run((a, (b, bs.toList)))
}

private object Sets {
  final val SAdd        = RedisCommand("SADD", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val SCard       = RedisCommand("SCARD", StringInput, LongOutput)
  final val SDiff       = RedisCommand("SDIFF", NonEmptyList(StringInput), ChunkOutput)
  final val SDiffStore  = RedisCommand("SDIFFSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val SInter      = RedisCommand("SINTER", NonEmptyList(StringInput), ChunkOutput)
  final val SInterStore = RedisCommand("SINTERSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val SIsMember   = RedisCommand("SISMEMBER", Tuple2(StringInput, StringInput), BoolOutput)
  final val SMembers    = RedisCommand("SMEMBERS", StringInput, ChunkOutput)
  final val SMove       = RedisCommand("SMOVE", Tuple3(StringInput, StringInput, StringInput), BoolOutput)
  final val SPop        = RedisCommand("SPOP", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput)

  final val SRandMember =
    RedisCommand("SRANDMEMBER", Tuple2(StringInput, OptionalInput(LongInput)), MultiStringChunkOutput)

  final val SRem = RedisCommand("SREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)

  final val SScan =
    RedisCommand(
      "SSCAN",
      Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
      ScanOutput
    )

  final val SUnion      = RedisCommand("SUNION", NonEmptyList(StringInput), ChunkOutput)
  final val SUnionStore = RedisCommand("SUNIONSTORE", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
}
