package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.Schema
import zio.{ Chunk, ZIO }

trait Sets {
  import Sets._

  /**
   * Add one or more members to a set
   *
   * @param key Key of set to add to
   * @param member first member to add
   * @param members subsequent members to add
   * @return Returns the number of elements that were added to the set, not including all the elements already present into the set
   */
  final def sAdd[K: Schema, M: Schema](key: K, member: M, members: M*): ZIO[RedisExecutor, RedisError, Long] =
    SAdd[K, M].run((key, (member, members.toList)))

  /**
   * Get the number of members in a set
   *
   * @param key Key of set to get the number of members of
   * @return Returns the cardinality (number of elements) of the set, or 0 if key does not exist
   */
  final def sCard[K: Schema](key: K): ZIO[RedisExecutor, RedisError, Long] =
    SCard[K].run(key)

  /**
   * Subtract multiple sets
   *
   * @param key Key of the set to subtract from
   * @param keys Keys of the sets to subtract
   * @return Returns the members of the set resulting from the difference between the first set and all the successive sets
   */
  final def sDiff[K: Schema, R: Schema](key: K, keys: K*): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SDiff[K, R].run((key, keys.toList))

  /**
   * Subtract multiple sets and store the resulting set in a key
   *
   * @param destination Key of set to store the resulting set
   * @param key Key of set to be subtracted from
   * @param keys Keys of sets to subtract
   * @return Returns the number of elements in the resulting set
   */
  final def sDiffStore[D: Schema, K: Schema](destination: D, key: K, keys: K*): ZIO[RedisExecutor, RedisError, Long] =
    SDiffStore[D, K].run((destination, (key, keys.toList)))

  /**
   * Intersect multiple sets and store the resulting set in a key
   *
   * @param destination Key of set to store the resulting set
   * @param keys Keys of the sets to intersect with each other
   * @return Returns the members of the set resulting from the intersection of all the given sets
   */
  final def sInter[K: Schema, R: Schema](destination: K, keys: K*): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SInter[K, R].run((destination, keys.toList))

  /**
   * Intersect multiple sets and store the resulting set in a key
   *
   * @param destination Key of set to store the resulting set
   * @param key Key of first set to intersect
   * @param keys Keys of subsequent sets to intersect
   * @return Returns the number of elements in the resulting set
   */
  final def sInterStore[D: Schema, K: Schema](destination: D, key: K, keys: K*): ZIO[RedisExecutor, RedisError, Long] =
    SInterStore[D, K].run((destination, (key, keys.toList)))

  /**
   * Determine if a given value is a member of a set
   *
   * @param key
   * @param member
   * @return Returns 1 if the element is a member of the set. 0 if the element is not a member of the set, or if key does not exist
   */
  final def sIsMember[K: Schema, M: Schema](key: K, member: M): ZIO[RedisExecutor, RedisError, Boolean] =
    SIsMember[K, M].run((key, member))

  /**
   * Get all the members in a set
   *
   * @param key Key of the set to get the members of
   * @return Returns the members of the set
   */
  final def sMembers[K: Schema, R: Schema](key: K): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SMembers[K, R].run(key)

  /**
   * Move a member from one set to another
   *
   * @param source Key of the set to move the member from
   * @param destination Key of the set to move the member to
   * @param member Element to move
   * @return Returns 1 if the element was moved. 0 if it was not found.
   */
  final def sMove[S: Schema, D: Schema, M: Schema](
    source: S,
    destination: D,
    member: M
  ): ZIO[RedisExecutor, RedisError, Boolean] =
    SMove[S, D, M].run((source, destination, member))

  /**
   * Remove and return one or multiple random members from a set
   *
   * @param key Key of the set to remove items from
   * @param count Number of elements to remove
   * @return Returns the elements removed
   */
  final def sPop[K: Schema, R: Schema](key: K, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SPop[K, R].run((key, count))

  /**
   * Get one or multiple random members from a set
   *
   * @param key Key of the set to get members from
   * @param count Number of elements to randomly get
   * @return Returns the random members
   */
  final def sRandMember[K: Schema, R: Schema](
    key: K,
    count: Option[Long] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SRandMember[K, R].run((key, count))

  /**
   * Remove one of more members from a set
   *
   * @param key Key of the set to remove members from
   * @param member Value of the first element to remove
   * @param members Subsequent values of elements to remove
   * @return Returns the number of members that were removed from the set, not including non existing members
   */
  final def sRem[K: Schema, M: Schema](key: K, member: M, members: M*): ZIO[RedisExecutor, RedisError, Long] =
    SRem[K, M].run((key, (member, members.toList)))

  /**
   * Incrementally iterate Set elements
   *
   * This is a cursor based scan of an entire set. Call initially with cursor set to 0 and on subsequent
   * calls pass the return value as the next cursor.
   *
   * @param key Key of the set to scan
   * @param cursor Cursor to use for this iteration of scan
   * @param pattern Glob-style pattern that filters which elements are returned
   * @param count Count of elements. Roughly this number will be returned by Redis if possible
   * @return Returns the next cursor, and items for this iteration or nothing when you reach the end, as a tuple
   */
  final def sScan[K: Schema, R: Schema](
    key: K,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (Long, Chunk[R])] =
    SScan[K, R].run((key, cursor, pattern.map(Pattern), count))

  /**
   * Add multiple sets
   *
   * @param key Key of the first set to add
   * @param keys Keys of the subsequent sets to add
   * @return Returns a list with members of the resulting set
   */
  final def sUnion[K: Schema, R: Schema](key: K, keys: K*): ZIO[RedisExecutor, RedisError, Chunk[R]] =
    SUnion[K, R].run((key, keys.toList))

  /**
   * Add multiple sets and add the resulting set in a key
   *
   * @param destination Key of destination to store the result
   * @param key Key of first set to add
   * @param keys Subsequent keys of sets to add
   * @return Returns the number of elements in the resulting set
   */
  final def sUnionStore[D: Schema, K: Schema](destination: D, key: K, keys: K*): ZIO[RedisExecutor, RedisError, Long] =
    SUnionStore[D, K].run((destination, (key, keys.toList)))
}

private[redis] object Sets {

  final def SAdd[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Long] =
    RedisCommand("SADD", Tuple2(ArbitraryInput(), NonEmptyList(ArbitraryInput())), LongOutput)

  final def SCard[K: Schema]: RedisCommand[K, Long] =
    RedisCommand("SCARD", ArbitraryInput(), LongOutput)

  final def SDiff[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand("SDIFF", NonEmptyList(ArbitraryInput()), ChunkOutput(ArbitraryOutput[R]()))

  final def SDiffStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] =
    RedisCommand("SDIFFSTORE", Tuple2(ArbitraryInput(), NonEmptyList(ArbitraryInput())), LongOutput)

  final def SInter[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand("SINTER", NonEmptyList(ArbitraryInput()), ChunkOutput(ArbitraryOutput[R]()))

  final def SInterStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] =
    RedisCommand("SINTERSTORE", Tuple2(ArbitraryInput(), NonEmptyList(ArbitraryInput())), LongOutput)

  final def SIsMember[K: Schema, M: Schema]: RedisCommand[(K, M), Boolean] =
    RedisCommand("SISMEMBER", Tuple2(ArbitraryInput(), ArbitraryInput()), BoolOutput)

  final def SMembers[K: Schema, R: Schema]: RedisCommand[K, Chunk[R]] =
    RedisCommand("SMEMBERS", ArbitraryInput(), ChunkOutput(ArbitraryOutput[R]()))

  final def SMove[S: Schema, D: Schema, M: Schema]: RedisCommand[(S, D, M), Boolean] =
    RedisCommand("SMOVE", Tuple3(ArbitraryInput(), ArbitraryInput(), ArbitraryInput()), BoolOutput)

  final def SPop[K: Schema, R: Schema]: RedisCommand[(K, Option[Long]), Chunk[R]] =
    RedisCommand(
      "SPOP",
      Tuple2(ArbitraryInput(), OptionalInput(LongInput)),
      MultiStringChunkOutput(ArbitraryOutput[R]())
    )

  final def SRandMember[K: Schema, R: Schema]: RedisCommand[(K, Option[Long]), Chunk[R]] =
    RedisCommand(
      "SRANDMEMBER",
      Tuple2(ArbitraryInput(), OptionalInput(LongInput)),
      MultiStringChunkOutput(ArbitraryOutput[R]())
    )

  final def SRem[K: Schema, M: Schema]: RedisCommand[(K, (M, List[M])), Long] =
    RedisCommand("SREM", Tuple2(ArbitraryInput(), NonEmptyList(ArbitraryInput())), LongOutput)

  final def SScan[K: Schema, R: Schema]: RedisCommand[(K, Long, Option[Pattern], Option[Count]), (Long, Chunk[R])] =
    RedisCommand(
      "SSCAN",
      Tuple4(ArbitraryInput(), LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
      ScanOutput(ArbitraryOutput[R]())
    )

  final def SUnion[K: Schema, R: Schema]: RedisCommand[(K, List[K]), Chunk[R]] =
    RedisCommand("SUNION", NonEmptyList(ArbitraryInput()), ChunkOutput(ArbitraryOutput[R]()))

  final def SUnionStore[D: Schema, K: Schema]: RedisCommand[(D, (K, List[K])), Long] =
    RedisCommand("SUNIONSTORE", Tuple2(ArbitraryInput(), NonEmptyList(ArbitraryInput())), LongOutput)
}
