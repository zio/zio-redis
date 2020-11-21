package zio.redis.api

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait Sets {
  import Sets._

  /** Add one or more members to a set
   *
    * @param key Key of set to add to
   * @param firstMember first member to add
   * @param restMembers subsequent members to add
   * @return Returns the number of elements that were added to the set, not including all the elements already present into the set
   */
  final def sAdd(key: String, firstMember: String, restMembers: String*): ZIO[RedisExecutor, RedisError, Long] =
    SAdd.run((key, (firstMember, restMembers.toList)))

  /** Get the number of members in a set
   *
    * @param key Key of set to get the number of members of
   * @return Returns the cardinality (number of elements) of the set, or 0 if key does not exist
   */
  final def sCard(key: String): ZIO[RedisExecutor, RedisError, Long] = SCard.run(key)

  /** Subtract multiple sets
   *
    * @param firstKey Key of the set to subtract from
   * @param restKeys Keys of the sets to subtract
   * @return Returns the members of the set resulting from the difference between the first set and all the successive sets
   */
  final def sDiff(firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SDiff.run((firstKey, restKeys.toList))

  /** Subtract multiple sets and store the resulting set in a key
   *
    * @param destination Key of set to store the resulting set
   * @param firstKey Key of set to be subtracted from
   * @param restKeys Keys of sets to subtract
   * @return Returns the number of elements in the resulting set
   */
  final def sDiffStore(destination: String, firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Long] =
    SDiffStore.run((destination, (firstKey, restKeys.toList)))

  /** Intersect multiple sets and store the resulting set in a key
   *
    * @param destination Key of set to store the resulting set
   * @param keys Keys of the sets to intersect with each other
   * @return Returns the members of the set resulting from the intersection of all the given sets
   */
  final def sInter(destination: String, keys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SInter.run((destination, keys.toList))

  /** Intersect multiple sets and store the resulting set in a key
   *
    * @param destination Key of set to store the resulting set
   * @param firstKey Key of first set to intersect
   * @param restKeys Keys of subsequent sets to intersect
   * @return Returns the number of elements in the resulting set
   */
  final def sInterStore(
    destination: String,
    firstKey: String,
    restKeys: String*
  ): ZIO[RedisExecutor, RedisError, Long] =
    SInterStore.run((destination, (firstKey, restKeys.toList)))

  /** Determine if a given value is a member of a set
   *
    * @param key
   * @param member
   * @return Returns 1 if the element is a member of the set. 0 if the element is not a member of the set, or if key does not exist
   */
  final def sIsMember(key: String, member: String): ZIO[RedisExecutor, RedisError, Boolean] =
    SIsMember.run((key, member))

  /** Get all the members in a set
   *
    * @param key Key of the set to get the members of
   * @return Returns the members of the set
   */
  final def sMembers(key: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = SMembers.run(key)

  /** Move a member from one set to another
   *
    * @param source Key of the set to move the member from
   * @param destination Key of the set to move the member to
   * @param member Element to move
   * @return Returns 1 if the element was moved. 0 if it was not found.
   */
  final def sMove(source: String, destination: String, member: String): ZIO[RedisExecutor, RedisError, Boolean] =
    SMove.run((source, destination, member))

  /** Remove and return one or multiple random members from a set
   *
    * @param key Key of the set to remove items from
   * @param count Number of elements to remove
   * @return Returns the elements removed
   */
  final def sPop(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SPop.run((key, count))

  /** Get one or multiple random members from a set
   *
    * @param key Key of the set to get members from
   * @param count Number of elements to randomly get
   * @return Returns the random members
   */
  final def sRandMember(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SRandMember.run((key, count))

  /** Remove one of more members from a set
   *
    * @param key Key of the set to remove members from
   * @param firstMember Value of the first element to remove
   * @param restMembers Subsequent values of elements to remove
   * @return Returns the number of members that were removed from the set, not including non existing members
   */
  final def sRem(key: String, firstMember: String, restMembers: String*): ZIO[RedisExecutor, RedisError, Long] =
    SRem.run((key, (firstMember, restMembers.toList)))

  /** Incrementally iterate Set elements
   *
    * This is a cursor based scan of an entire set. Call initially with cursor set to 0 and on subsequent
   * calls pass the return value as the next cursor.
   *
    * @param key Key of the set to scan
   * @param cursor Cursor to use for this iteration of scan
   * @param regex Glob-style pattern that filters which elements are returned
   * @param count Count of elements. Roughly this number will be returned by Redis if possible
   * @return Returns the items for this iteration or nothing when you reach the end
   */
  final def sScan(
    key: String,
    cursor: Long,
    regex: Option[Regex] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = SScan.run((key, cursor, regex, count))

  /**
   * Add multiple sets
   *
    * @param firstKey Key of the first set to add
   * @param restKeys Keys of the subsequent sets to add
   * @return Returns a list with members of the resulting set
   */
  final def sUnion(firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    SUnion.run((firstKey, restKeys.toList))

  /** Add multiple sets and add the resulting set in a key
   *
    * @param destination Key of destination to store the result
   * @param firstKey Key of first set to add
   * @param restKeys Subsequent keys of sets to add
   * @return Returns the number of elements in the resulting set
   */
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
