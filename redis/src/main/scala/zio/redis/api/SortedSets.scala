package zio.redis.api

import zio.duration._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }
import zio.schema.Schema

trait SortedSets {
  import SortedSets._

  /**
   * Remove and return the member with the highest score from one or more sorted sets, or block until one is available.
   *
   * @param timeout Maximum number of seconds to block. A timeout of zero can be used to block indefinitely
   * @param key Key of the set
   * @param keys Keys of the rest sets
   * @return A three-element Chunk with the first element being the name of the key where a member was popped,
   *         the second element is the popped member itself, and the third element is the score of the popped element.
   *         An empty chunk is returned when no element could be popped and the timeout expired.
   */

  final def bzPopMax(
    timeout: Duration,
    key: String,
    keys: String*
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command =
      RedisCommand(BzPopMax, Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput(MultiStringOutput))
    command.run((timeout, (key, keys.toList)))
  }

  /**
   * Remove and return the member with the lowest score from one or more sorted sets, or block until one is available.
   *
   * @param timeout Maximum number of seconds to block. A timeout of zero can be used to block indefinitely
   * @param key Key of the set
   * @param keys Keys of the rest sets
   * @return A three-element Chunk with the first element being the name of the key where a member was popped,
   *         the second element is the popped member itself, and the third element is the score of the popped element.
   *         An empty chunk is returned when no element could be popped and the timeout expired.
   */
  final def bzPopMin(
    timeout: Duration,
    key: String,
    keys: String*
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command =
      RedisCommand(BzPopMin, Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput(MultiStringOutput))
    command.run((timeout, (key, keys.toList)))
  }

  /**
   * Add one or more members to a sorted set, or update its score if it already exists.
   *
   * @param key Key of set to add to
   * @param update Set existing and never add elements or always set new elements and don't update existing elements
   * @param change Modify the return value from the number of new elements added, to the total number of elements change
   * @param memberScore Score that should be added to specific element for a given sorted set key
   * @param memberScores Rest scores that should be added to specific elements fr a given sorted set key
   * @return The number of elements added to the sorted set, not including elements already existing for which the score was updated
   */
  final def zAdd[K: Schema, M: Schema](key: K, update: Option[Update] = None, change: Option[Changed] = None)(
    memberScore: MemberScore[M],
    memberScores: MemberScore[M]*
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      ZAdd,
      Tuple4(
        ArbitraryInput[K](),
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        NonEmptyList(MemberScoreInput[M]())
      ),
      LongOutput
    )
    command.run((key, update, change, (memberScore, memberScores.toList)))
  }

  /**
   * Add one or more members to a sorted set, or update its score if it already exists.
   *
   * @param key Key of set to add to.
   * @param update Set existing and never add elements or always set new elements and don't update existing elements
   * @param change Modify the return value from the number of new elements added, to the total number of elements change
   * @param increment When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode
   * @param memberScore Score that should be added to specific element for a given sorted set key
   * @param memberScores Rest scores that should be added to specific elements fr a given sorted set key
   * @return The new score of member (a double precision floating point number), or None if the operation was aborted (when called with either the XX or the NX option)
   */
  final def zAddWithIncr[K: Schema, M: Schema](key: K, update: Option[Update] = None, change: Option[Changed] = None)(
    increment: Increment,
    memberScore: MemberScore[M],
    memberScores: MemberScore[M]*
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = {
    val command = RedisCommand(
      ZAdd,
      Tuple5(
        ArbitraryInput[K](),
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        IncrementInput,
        NonEmptyList(MemberScoreInput[M]())
      ),
      OptionalOutput(DoubleOutput)
    )
    command.run((key, update, change, increment, (memberScore, memberScores.toList)))
  }

  /**
   * Get the number of members in a sorted set.
   *
   * @param key Key of a sorted set
   * @return The cardinality (number of elements) of the sorted set, or 0 if key does not exist
   */
  final def zCard(key: String): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZCard, StringInput, LongOutput)
    command.run(key)
  }

  /**
   * Returns the number of elements in the sorted set at key with a score between min and max.
   *
   * @param key Key of a sorted set
   * @param range Min and max score (including elements with score equal to min or max)
   * @return the number of elements in the specified score range
   */
  final def zCount(key: String, range: Range): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZCount, Tuple2(StringInput, RangeInput), LongOutput)
    command.run((key, range))
  }

  /**
   * Increment the score of a member in a sorted set.
   *
   * @param key Key of a sorted set
   * @param increment Increment value
   * @param member Member of sorted set
   * @return The new score of member (a double precision floating point number)
   */
  final def zIncrBy(key: String, increment: Long, member: String): ZIO[RedisExecutor, RedisError, Double] = {
    val command = RedisCommand(ZIncrBy, Tuple3(StringInput, LongInput, StringInput), DoubleOutput)
    command.run((key, increment, member))
  }

  /**
   * Intersect multiple sorted sets and store the resulting sorted set in a new key.
   *
   * @param destination Key of the output
   * @param inputKeysNum Number of input keys
   * @param key Key of a sorted set
   * @param keys Keys of the rest sorted sets
   * @param aggregate With the AGGREGATE option, it is possible to specify how the results of the union are aggregated
   * @param weights Represents WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set.
   *          This means that the score of every element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
   *          When WEIGHTS is not given, the multiplication factors default to 1.
   * @return The number of elements in the resulting sorted set at destination
   */
  final def zInterStore(destination: String, inputKeysNum: Long, key: String, keys: String*)(
    aggregate: Option[Aggregate] = None,
    weights: Option[::[Double]] = None
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      ZInterStore,
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(AggregateInput),
        OptionalInput(WeightsInput)
      ),
      LongOutput
    )
    command.run((destination, inputKeysNum, (key, keys.toList), aggregate, weights))
  }

  /**
   * Count the number of members in a sorted set between a given lexicographical range.
   *
   * @param key Key of a sorted set
   * @param lexRange LexRange that must be satisfied
   * @return The number of elements in the specified score range
   */
  final def zLexCount(key: String, lexRange: LexRange): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZLexCount, Tuple2(StringInput, LexRangeInput), LongOutput)
    command.run((key, lexRange))
  }

  /**
   * Remove and return members with the highest scores in a sorted set.
   *
   * @param key Key of a sorted set
   * @param count When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
   *          When returning multiple elements, the one with the highest score will be the first, followed by the elements with lower scores.
   * @return Chunk of popped elements and scores
   */
  final def zPopMax(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(ZPopMax, Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput(MultiStringOutput))
    command.run((key, count))
  }

  /**
   * Remove and return members with the lowest scores in a sorted set.
   *
   * @param key Key of a sorted set
   * @param count When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
   *          When returning multiple elements, the one with the lowest score will be the first, followed by the elements with greater scores.
   * @return Chunk of popped elements and scores
   */
  final def zPopMin(key: String, count: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(ZPopMin, Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput(MultiStringOutput))
    command.run((key, count))
  }

  /**
   * Return a range of members in a sorted set, by index.
   *
   * @param key Key of a sorted set
   * @param range Inclusive range
   * @param withScores The optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone
   * @return Chunk of elements in the specified range (optionally with their scores, in case the WITHSCORES option is given)
   */
  final def zRange(
    key: String,
    range: Range,
    withScores: Option[WithScores] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRange,
      Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, range, withScores))
  }

  /**
   * Return a range of members in a sorted set, by lexicographical range.
   *
   * @param key Key of a sorted set
   * @param lexRange LexRange that must be satisfied
   * @param limit The optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset
   * @return Chunk of elements in the specified score range
   */
  final def zRangeByLex(
    key: String,
    lexRange: LexRange,
    limit: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRangeByLex,
      Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, lexRange, limit))
  }

  /**
   * Return a range of members in a sorted set, by score.
   *
   * @param key Key of a sorted set
   * @param scoreRange ScoreRange that must be satisfied
   * @param withScores The optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone
   * @param limit The optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset
   * @return Chunk of elements in the specified score range (optionally with their scores)
   */
  final def zRangeByScore(
    key: String,
    scoreRange: ScoreRange,
    withScores: Option[WithScores] = None,
    limit: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRangeByScore,
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, scoreRange, withScores, limit))
  }

  /**
   * Determine the index of a member in a sorted set.
   *
   * @param key Key of a sorted set
   * @param member Member of sorted set
   * @return The rank of member in the sorted set stored at key, with the scores ordered from low to high
   */
  final def zRank(key: String, member: String): ZIO[RedisExecutor, RedisError, Option[Long]] = {
    val command = RedisCommand(ZRank, Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))
    command.run((key, member))
  }

  /**
   * Remove one or more members from a sorted set.
   *
   * @param key Key of a sorted set
   * @param firstMember Member to be removed
   * @param restMembers Rest members to be removed
   * @return The number of members removed from the sorted set, not including non existing members
   */
  final def zRem(key: String, firstMember: String, restMembers: String*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZRem, Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
    command.run((key, (firstMember, restMembers.toList)))
  }

  /**
   * Remove all members in a sorted set between the given lexicographical range.
   *
   * @param key Key of a sorted set
   * @param lexRange LexRange that must be satisfied
   * @return The number of elements removed
   */
  final def zRemRangeByLex(key: String, lexRange: LexRange): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZRemRangeByLex, Tuple2(StringInput, LexRangeInput), LongOutput)
    command.run((key, lexRange))
  }

  /**
   * Remove all members in a sorted set within the given indexes.
   *
   * @param key Key of a sorted set
   * @param range Range that must be satisfied
   * @return The number of elements removed
   */
  final def zRemRangeByRank(key: String, range: Range): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZRemRangeByRank, Tuple2(StringInput, RangeInput), LongOutput)
    command.run((key, range))
  }

  /**
   * Remove all members in a sorted set within the given scores.
   *
   * @param key Key of a sorted set
   * @param scoreRange ScoreRange that must be satisfied
   * @return The number of elements removed
   */
  final def zRemRangeByScore(key: String, scoreRange: ScoreRange): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(ZRemRangeByScore, Tuple2(StringInput, ScoreRangeInput), LongOutput)
    command.run((key, scoreRange))
  }

  /**
   * Return a range of members in a sorted set, by index, with scores ordered from high to low.
   *
   * @param key Key of a sorted set
   * @param range Range that must be satisfied
   * @param withScores The optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone
   * @return Chunk of elements in the specified range (optionally with their scores)
   */
  final def zRevRange(
    key: String,
    range: Range,
    withScores: Option[WithScores] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRevRange,
      Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, range, withScores))
  }

  /**
   * Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
   *
   * @param key Key of a sorted set
   * @param lexRange LexRange that must be satisfied
   * @param limit The optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset
   * @return Chunk of elements in the specified score range
   */
  final def zRevRangeByLex(
    key: String,
    lexRange: LexRange,
    limit: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRevRangeByLex,
      Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, lexRange, limit))
  }

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low.
   *
   * @param key Key of a sorted set
   * @param scoreRange ScoreRange that must be satisfied
   * @param withScores The optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone
   * @param limit The optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset
   * @return Chunk of elements in the specified range (optionally with their scores)
   */
  final def zRevRangeByScore(
    key: String,
    scoreRange: ScoreRange,
    withScores: Option[WithScores] = None,
    limit: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(
      ZRevRangeByScore,
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput(MultiStringOutput)
    )
    command.run((key, scoreRange, withScores, limit))
  }

  /**
   * Determine the index of a member in a sorted set, with scores ordered from high to low.
   *
   * @param key Key of a sorted set
   * @param member Member of sorted set
   * @return The rank of member
   */
  final def zRevRank(key: String, member: String): ZIO[RedisExecutor, RedisError, Option[Long]] = {
    val command = RedisCommand(ZRevRank, Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))
    command.run((key, member))
  }

  /**
   * Incrementally iterate sorted sets elements and associated scores.
   *
   * @param key Key of the set to scan
   * @param cursor Cursor to use for this iteration of scan
   * @param pattern Glob-style pattern that filters which elements are returned
   * @param count Count of elements. Roughly this number will be returned by Redis if possible
   * @return Returns the items for this iteration or nothing when you reach the end
   */
  final def zScan(
    key: String,
    cursor: Long,
    pattern: Option[String] = None,
    count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (Long, Chunk[String])] = {
    val command = RedisCommand(
      ZScan,
      Tuple4(StringInput, LongInput, OptionalInput(PatternInput), OptionalInput(CountInput)),
      ScanOutput(MultiStringOutput)
    )
    command.run((key, cursor, pattern.map(Pattern), count))
  }

  /**
   * Get the score associated with the given member in a sorted set.
   *
   * @param key Key of a sorted set
   * @param member Member of sorted set
   * @return The score of member (a double precision floating point number
   */
  final def zScore(key: String, member: String): ZIO[RedisExecutor, RedisError, Option[Double]] = {
    val command = RedisCommand(ZScore, Tuple2(StringInput, StringInput), OptionalOutput(DoubleOutput))
    command.run((key, member))
  }

  /**
   * Add multiple sorted sets and store the resulting sorted set in a new key.
   *
   * @param destination Key of the output
   * @param inputKeysNum Number of input keys
   * @param key Key of a sorted set
   * @param keys Keys of other sorted sets
   * @param weights Represents WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set
   *          This means that the score of every element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
   *          When WEIGHTS is not given, the multiplication factors default to 1.
   * @param aggregate With the AGGREGATE option, it is possible to specify how the results of the union are aggregated
   * @return The number of elements in the resulting sorted set at destination
   */
  final def zUnionStore(destination: String, inputKeysNum: Long, key: String, keys: String*)(
    weights: Option[::[Double]] = None,
    aggregate: Option[Aggregate] = None
  ): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(
      ZUnionStore,
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(WeightsInput),
        OptionalInput(AggregateInput)
      ),
      LongOutput
    )
    command.run((destination, inputKeysNum, (key, keys.toList), weights, aggregate))
  }
}

private[redis] object SortedSets {
  final val BzPopMax         = "BZPOPMAX"
  final val BzPopMin         = "BZPOPMIN"
  final val ZAdd             = "ZADD"
  final val ZCard            = "ZCARD"
  final val ZCount           = "ZCOUNT"
  final val ZIncrBy          = "ZINCRBY"
  final val ZInterStore      = "ZINTERSTORE"
  final val ZLexCount        = "ZLEXCOUNT"
  final val ZPopMax          = "ZPOPMAX"
  final val ZPopMin          = "ZPOPMIN"
  final val ZRange           = "ZRANGE"
  final val ZRangeByLex      = "ZRANGEBYLEX"
  final val ZRangeByScore    = "ZRANGEBYSCORE"
  final val ZRank            = "ZRANK"
  final val ZRem             = "ZREM"
  final val ZRemRangeByLex   = "ZREMRANGEBYLEX"
  final val ZRemRangeByRank  = "ZREMRANGEBYRANK"
  final val ZRemRangeByScore = "ZREMRANGEBYSCORE"
  final val ZRevRange        = "ZREVRANGE"
  final val ZRevRangeByLex   = "ZREVRANGEBYLEX"
  final val ZRevRangeByScore = "ZREVRANGEBYSCORE"
  final val ZRevRank         = "ZREVRANK"
  final val ZScan            = "ZSCAN"
  final val ZScore           = "ZSCORE"
  final val ZUnionStore      = "ZUNIONSTORE"
}
