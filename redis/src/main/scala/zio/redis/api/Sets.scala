package zio.redis.api

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Sets {
  import Sets._

  // Add one or more members to a set
  final def sAdd(key: String, firstMember: String, restMembers: String*): ZIO[RedisExecutor, RedisError, Long] =
    SAdd.run((key, (firstMember, restMembers.toList)))

  // Get the number of members in a set
  final def sCard(key: String): ZIO[RedisExecutor, RedisError, Long] = SCard.run(key)

  // Subtract multiple sets
  final def sDiff(firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SDiff.run((firstKey, restKeys.toList))

  // Subtract multiple sets and store the resulting set in a key
  final def sDiffStore(destination: String, firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Long] =
    SDiffStore.run((destination, (firstKey, restKeys.toList)))

  // Intersect multiple sets and store the resulting set in a key
  final def sInter(destination: String, keys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SInter.run((destination, keys.toList))

  // Intersect multiple sets and store the resulting set in a key
  final def sInterStore(
    destination: String,
    firstKey: String,
    restKeys: String*
  ): ZIO[RedisExecutor, RedisError, Long] =
    SInterStore.run((destination, (firstKey, restKeys.toList)))

  // Determine is a given value is a member of a set
  final def sIsMember(key: String, member: String): ZIO[RedisExecutor, RedisError, Boolean] =
    SIsMember.run((key, member))

  // Get all the members in a set
  final def sMembers(key: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = SMembers.run(key)

  // Move a member from one set to another
  final def sMove(source: String, destination: String, member: String): ZIO[RedisExecutor, RedisError, Boolean] =
    SMove.run((source, destination, member))

  // Remove and return one or multiple random members from a set
  final def sPop(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SPop.run((key, count))

  // Get one or multiple random members from a set
  final def sRandMember(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SRandMember.run((key, count))

  // Remove one of more members from a set
  final def sRem(key: String, firstMember: String, restMembers: String*): ZIO[RedisExecutor, RedisError, Long] =
    SRem.run((key, (firstMember, restMembers.toList)))

  /** Incrementally iterate Set elements
   *
    * This is a cursor based iteration of the entire set. You pass an initial cursor of zero and you will receive
   * a number of elements back. You can give Redis a hint to the number you would like on each iteration using the
   * count parameter, but this is not guaranteed to be exact.
   * The result of the first call to Scan includes a cursor which you can use for the next call, and so on until the
   * cursor is zero.
   *
    * If you provide a regex, only set elements that match the regex will be included in the results.
   */
  final def sScan(
    key: String,
    cursor: Long,
    regex: Option[Regex] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = SScan.run((key, cursor, regex, count))

  // Add multiple sets
  final def sUnion(firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SUnion.run((firstKey, restKeys.toList))

  // Add multiple sets and add the resulting set in a key
  final def sUnionStore(
    destination: String,
    firstKey: String,
    restKeys: String*
  ): ZIO[RedisExecutor, RedisError, Long] =
    SUnionStore.run((destination, (firstKey, restKeys.toList)))
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
