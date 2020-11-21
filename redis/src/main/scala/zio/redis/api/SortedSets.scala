package zio.redis.api

import java.time.Duration

import scala.util.matching.Regex

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait SortedSets {
  import SortedSets._

  /**
   * Remove and return the member with the highest score from one or more sorted sets, or block until one is available.
   *
   * @param timeout Maximum number of seconds to block. A timeout of zero can be used to block indefinitely.
   * @param firstKey Key of the set
   * @param restKeys Keys of the rest sets
   * @return A three-element Chunk with the first element being the name of the key where a member was popped,
   *         the second element is the popped member itself, and the third element is the score of the popped element.
   *         An empty chunk is returned when no element could be popped and the timeout expired.
   */
  final def bzPopMax(timeout: Duration, firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    BzPopMax.run((timeout, (firstKey, restKeys.toList)))

  /**
   * Remove and return the member with the lowest score from one or more sorted sets, or block until one is available.
   *
   * @param timeout Maximum number of seconds to block. A timeout of zero can be used to block indefinitely.
   * @param firstKey Key of the set
   * @param restKeys Keys of the rest sets
   * @return A three-element Chunk with the first element being the name of the key where a member was popped,
   *         the second element is the popped member itself, and the third element is the score of the popped element.
   *         An empty chunk is returned when no element could be popped and the timeout expired.
   */
  final def bzPopMin(timeout: Duration, firstKey: String, restKeys: String*): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    BzPopMin.run((timeout, (firstKey, restKeys.toList)))

  /**
   * Add one or more members to a sorted set, or update its score if it already exists.
   *
   * @param key Key of set to add to
   * @param update Set existing and never add elements or always set new elements and don't update existing elements
   * @param change Modify the return value from the number of new elements added, to the total number of elements change
   * @param firstMemberScore Score that should be added to specific element for a given sorted set key
   * @param restMemberScores Rest scores that should be added to specific elements fr a given sorted set key
   * @return The number of elements added to the sorted set, not including elements already existing for which the score was updated
   */
  final def zAdd(key: String, update: Option[Update] = None, change: Option[Changed] = None)(
    firstMemberScore: MemberScore,
    restMemberScores: MemberScore*
  ): ZIO[RedisExecutor, RedisError, Long] = ZAdd.run((key, update, change, (firstMemberScore, restMemberScores.toList)))

  /**
   * Add one or more members to a sorted set, or update its score if it already exists.
   *
   * @param key Key of set to add to.
   * @param update Set existing and never add elements or always set new elements and don't update existing elements
   * @param change Modify the return value from the number of new elements added, to the total number of elements change
   * @param increment When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode
   * @param firstMemberScore Score that should be added to specific element for a given sorted set key
   * @param restMemberScores Rest scores that should be added to specific elements fr a given sorted set key
   * @return The new score of member (a double precision floating point number), or None if the operation was aborted (when called with either the XX or the NX option)
   */
  final def zAddWithIncr(key: String, update: Option[Update] = None, change: Option[Changed] = None)(
    increment: Increment,
    firstMemberScore: MemberScore,
    restMemberScores: MemberScore*
  ): ZIO[RedisExecutor, RedisError, Option[Double]] = ZAddWithIncr.run((key, update, change, increment, (firstMemberScore, restMemberScores.toList)))

  /**
   * Get the number of members in a sorted set.
   *
   * @param key Key of a sorted set.
   * @return the cardinality (number of elements) of the sorted set, or 0 if key does not exist.
   */
  final def zCard(key: String): ZIO[RedisExecutor, RedisError, Long] = ZCard.run(key)

  /**
   * Returns the number of elements in the sorted set at key with a score between min and max.
   *
   * @param a key of a sorted set.
   * @param b min and max score (including elements with score equal to min or max).
   * @return the number of elements in the specified score range.
   */
  final def zCount(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = ZCount.run((a, b))

  /**
   * Increment the score of a member in a sorted set.
   *
   * @param a key of a sorted set.
   * @param b increment value.
   * @param c member of sorted set.
   * @return the new score of member (a double precision floating point number).
   */
  final def zIncrBy(a: String, b: Long, c: String): ZIO[RedisExecutor, RedisError, Double] = ZIncrBy.run((a, b, c))

  /**
   * Intersect multiple sorted sets and store the resulting sorted set in a new key.
   *
   * @param a key of the output.
   * @param b number of input keys.
   * @param c key of a sorted set.
   * @param cs keys of other sorted sets.
   * @param d with the AGGREGATE option, it is possible to specify how the results of the union are aggregated.
   * @param e represents WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set.
   *          This means that the score of every element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
   *          When WEIGHTS is not given, the multiplication factors default to 1.
   * @return the number of elements in the resulting sorted set at destination.
   */
  final def zInterStore(a: String, b: Long, c: String, cs: String*)(
    d: Option[Aggregate] = None,
    e: Option[::[Double]] = None
  ): ZIO[RedisExecutor, RedisError, Long] = ZInterStore.run((a, b, (c, cs.toList), d, e))

  /**
   * Count the number of members in a sorted set between a given lexicographical range.
   *
   * @param a key of a sorted set.
   * @param b LexRange that must be satisfied.
   * @return the number of elements in the specified score range.
   */
  final def zLexCount(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = ZLexCount.run((a, b))

  /**
   * Remove and return members with the highest scores in a sorted set.
   *
   * @param a key of a sorted set.
   * @param b When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
   *          When returning multiple elements, the one with the highest score will be the first, followed by the elements with lower scores.
   * @return Chunk of popped elements and scores.
   */
  final def zPopMax(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZPopMax.run((a, b))

  /**
   * Remove and return members with the lowest scores in a sorted set.
   *
   * @param a key of a sorted set.
   * @param b When left unspecified, the default value for count is 1. Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
   *          When returning multiple elements, the one with the lowest score will be the first, followed by the elements with greater scores.
   * @return Chunk of popped elements and scores.
   */
  final def zPopMin(a: String, b: Option[Long] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZPopMin.run((a, b))

  /**
   * Return a range of members in a sorted set, by index.
   *
   * @param a key of a sorted set.
   * @param b inclusive range.
   * @param c the optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone.
   * @return Chunk of elements in the specified range (optionally with their scores, in case the WITHSCORES option is given).
   */
  final def zRange(a: String, b: Range, c: Option[WithScores] = None): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRange.run((a, b, c))

  /**
   * Return a range of members in a sorted set, by lexicographical range.
   *
   * @param a key of a sorted set.
   * @param b LexRange that must be satisfied.
   * @param c the optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset.
   * @return Chunk of elements in the specified score range.
   */
  final def zRangeByLex(
    a: String,
    b: LexRange,
    c: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRangeByLex.run((a, b, c))

  /**
   * Return a range of members in a sorted set, by score.
   *
   * @param a key of a sorted set.
   * @param b ScoreRange that must be satisfied.
   * @param c the optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone.
   * @param d the optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset.
   * @return Chunk of elements in the specified score range (optionally with their scores).
   */
  final def zRangeByScore(
    a: String,
    b: ScoreRange,
    c: Option[WithScores] = None,
    d: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] = ZRangeByScore.run((a, b, c, d))

  /**
   * Determine the index of a member in a sorted set.
   *
   * @param a key of a sorted set.
   * @param b member of sorted set.
   * @return the rank of member in the sorted set stored at key, with the scores ordered from low to high.
   */
  final def zRank(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = ZRank.run((a, b))

  /**
   * Remove one or more members from a sorted set.
   *
   * @param a key of a sorted set.
   * @param b member to be removed.
   * @param bs other members to be removed.
   * @return the number of members removed from the sorted set, not including non existing members.
   */
  final def zRem(a: String, b: String, bs: String*): ZIO[RedisExecutor, RedisError, Long] =
    ZRem.run((a, (b, bs.toList)))

  /**
   * Remove all members in a sorted set between the given lexicographical range.
   *
   * @param a key of a sorted set.
   * @param b LexRange that must be satisfied.
   * @return the number of elements removed.
   */
  final def zRemRangeByLex(a: String, b: LexRange): ZIO[RedisExecutor, RedisError, Long] = ZRemRangeByLex.run((a, b))

  /**
   * Remove all members in a sorted set within the given indexes.
   *
   * @param a key of a sorted set.
   * @param b Range that must be satisfied.
   * @return the number of elements removed.
   */
  final def zRemRangeByRank(a: String, b: Range): ZIO[RedisExecutor, RedisError, Long] = ZRemRangeByRank.run((a, b))

  /**
   * Remove all members in a sorted set within the given scores.
   *
   * @param a key of a sorted set.
   * @param b ScoreRange that must be satisfied.
   * @return the number of elements removed.
   */
  final def zRemRangeByScore(a: String, b: ScoreRange): ZIO[RedisExecutor, RedisError, Long] =
    ZRemRangeByScore.run((a, b))

  /**
   * Return a range of members in a sorted set, by index, with scores ordered from high to low.
   *
   * @param a key of a sorted set.
   * @param b Range that must be satisfied.
   * @param c the optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone.
   * @return Chunk of elements in the specified range (optionally with their scores).
   */
  final def zRevRange(
    a: String,
    b: Range,
    c: Option[WithScores] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRange.run((a, b, c))

  /**
   * Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
   *
   * @param a key of a sorted set.
   * @param b LexRange that must be satisfied.
   * @param c the optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset.
   * @return Chunk of elements in the specified score range.
   */
  final def zRevRangeByLex(
    a: String,
    b: LexRange,
    c: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRangeByLex.run((a, b, c))

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low.
   *
   * @param a key of a sorted set.
   * @param b ScoreRange that must be satisfied.
   * @param c the optional WITHSCORES argument makes the command return both the element and its score, instead of the element alone.
   * @param d the optional LIMIT argument can be used to only get a range of the matching elements. A negative count returns all elements from the offset.
   * @return Chunk of elements in the specified range (optionally with their scores).
   */
  final def zRevRangeByScore(
    a: String,
    b: ScoreRange,
    c: Option[WithScores] = None,
    d: Option[Limit] = None
  ): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    ZRevRangeByScore.run((a, b, c, d))

  /**
   * Determine the index of a member in a sorted set, with scores ordered from high to low.
   *
   * @param a key of a sorted set.
   * @param b member of sorted set.
   * @return the rank of member.
   */
  final def zRevRank(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Long]] = ZRevRank.run((a, b))

  /**
   * Incrementally iterate sorted sets elements and associated scores.
   *
   * @param key Key of the set to scan
   * @param cursor Cursor to use for this iteration of scan
   * @param regex Glob-style pattern that filters which elements are returned
   * @param count Count of elements. Roughly this number will be returned by Redis if possible
   * @return Returns the items for this iteration or nothing when you reach the end
   */
  final def zScan(
                   key: String,
                   cursor: Long,
                   regex: Option[Regex] = None,
                   count: Option[Count] = None
  ): ZIO[RedisExecutor, RedisError, (String, Chunk[String])] = ZScan.run((key, cursor, regex, count))

  /**
   * Get the score associated with the given member in a sorted set.
   *
   * @param a key of a sorted set.
   * @param b member of sorted set.
   * @return  the score of member (a double precision floating point number.
   */
  final def zScore(a: String, b: String): ZIO[RedisExecutor, RedisError, Option[Double]] = ZScore.run((a, b))

  /**
   * Add multiple sorted sets and store the resulting sorted set in a new key.
   *
   * @param a key of the output.
   * @param b number of input keys.
   * @param c key of a sorted set.
   * @param cs keys of other sorted sets.
   * @param d represents WEIGHTS option, it is possible to specify a multiplication factor for each input sorted set.
   *          This means that the score of every element in every input sorted set is multiplied by this factor before being passed to the aggregation function.
   *          When WEIGHTS is not given, the multiplication factors default to 1.
   * @param e with the AGGREGATE option, it is possible to specify how the results of the union are aggregated.
   * @return the number of elements in the resulting sorted set at destination.
   */
  final def zUnionStore(a: String, b: Long, c: String, cs: String*)(
    d: Option[::[Double]] = None,
    e: Option[Aggregate] = None
  ): ZIO[RedisExecutor, RedisError, Long] = ZUnionStore.run((a, b, (c, cs.toList), d, e))
}

private object SortedSets {
  final val BzPopMax =
    RedisCommand("BZPOPMAX", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)
  final val BzPopMin =
    RedisCommand("BZPOPMIN", Tuple2(DurationSecondsInput, NonEmptyList(StringInput)), ChunkOutput)

  final val ZAdd =
    RedisCommand(
      "ZADD",
      Tuple4(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        NonEmptyList(MemberScoreInput)
      ),
      LongOutput
    )

  final val ZAddWithIncr =
    RedisCommand(
      "ZADD",
      Tuple5(
        StringInput,
        OptionalInput(UpdateInput),
        OptionalInput(ChangedInput),
        IncrementInput,
        NonEmptyList(MemberScoreInput)
      ),
      OptionalOutput(DoubleOutput)
    )

  final val ZCard   = RedisCommand("ZCARD", StringInput, LongOutput)
  final val ZCount  = RedisCommand("ZCOUNT", Tuple2(StringInput, RangeInput), LongOutput)
  final val ZIncrBy = RedisCommand("ZINCRBY", Tuple3(StringInput, LongInput, StringInput), DoubleOutput)

  final val ZInterStore =
    RedisCommand(
      "ZINTERSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(AggregateInput),
        OptionalInput(WeightsInput)
      ),
      LongOutput
    )

  final val ZLexCount = RedisCommand("ZLEXCOUNT", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val ZPopMax   = RedisCommand("ZPOPMAX", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)
  final val ZPopMin   = RedisCommand("ZPOPMIN", Tuple2(StringInput, OptionalInput(LongInput)), ChunkOutput)

  final val ZRange =
    RedisCommand("ZRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val ZRangeByLex =
    RedisCommand("ZRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val ZRangeByScore =
    RedisCommand(
      "ZRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    )

  final val ZRank            = RedisCommand("ZRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))
  final val ZRem             = RedisCommand("ZREM", Tuple2(StringInput, NonEmptyList(StringInput)), LongOutput)
  final val ZRemRangeByLex   = RedisCommand("ZREMRANGEBYLEX", Tuple2(StringInput, LexRangeInput), LongOutput)
  final val ZRemRangeByRank  = RedisCommand("ZREMRANGEBYRANK", Tuple2(StringInput, RangeInput), LongOutput)
  final val ZRemRangeByScore = RedisCommand("ZREMRANGEBYSCORE", Tuple2(StringInput, ScoreRangeInput), LongOutput)

  final val ZRevRange =
    RedisCommand("ZREVRANGE", Tuple3(StringInput, RangeInput, OptionalInput(WithScoresInput)), ChunkOutput)

  final val ZRevRangeByLex =
    RedisCommand("ZREVRANGEBYLEX", Tuple3(StringInput, LexRangeInput, OptionalInput(LimitInput)), ChunkOutput)

  final val ZRevRangeByScore =
    RedisCommand(
      "ZREVRANGEBYSCORE",
      Tuple4(StringInput, ScoreRangeInput, OptionalInput(WithScoresInput), OptionalInput(LimitInput)),
      ChunkOutput
    )

  final val ZRevRank = RedisCommand("ZREVRANK", Tuple2(StringInput, StringInput), OptionalOutput(LongOutput))

  final val ZScan =
    RedisCommand(
      "ZSCAN",
      Tuple4(StringInput, LongInput, OptionalInput(RegexInput), OptionalInput(CountInput)),
      ScanOutput
    )

  final val ZScore = RedisCommand("ZSCORE", Tuple2(StringInput, StringInput), OptionalOutput(DoubleOutput))

  final val ZUnionStore =
    RedisCommand(
      "ZUNIONSTORE",
      Tuple5(
        StringInput,
        LongInput,
        NonEmptyList(StringInput),
        OptionalInput(WeightsInput),
        OptionalInput(AggregateInput)
      ),
      LongOutput
    )
}
