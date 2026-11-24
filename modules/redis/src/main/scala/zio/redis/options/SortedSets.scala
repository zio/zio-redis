/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.options

import zio.Chunk

/**
 * Types and flags for the sorted set commands, the `Z` commands.
 */
trait SortedSets {

  /**
   * How `ZUNIONSTORE`, `ZINTERSTORE` and the related commands combine the scores of a member that appears in
   * more than one input set.
   */
  sealed trait Aggregate extends Product { self =>
    private[redis] final def asString: String =
      self match {
        case Aggregate.Max => "MAX"
        case Aggregate.Min => "MIN"
        case Aggregate.Sum => "SUM"
      }
  }

  /**
   * The aggregation functions for the `AGGREGATE` option.
   */
  object Aggregate {

    /**
     * Adds the scores of the member. Sent as `SUM`.
     */
    case object Sum extends Aggregate

    /**
     * Keeps the smallest score of the member. Sent as `MIN`.
     */
    case object Min extends Aggregate

    /**
     * Keeps the largest score of the member. Sent as `MAX`.
     */
    case object Max extends Aggregate
  }

  /**
   * The `MAX` argument of `ZMPOP` and `BZMPOP`, which pop the members with the highest scores. Sent as `MAX`.
   */
  case object Max {
    private[redis] def asSring: String = "MAX"
  }

  /**
   * The type of the `Max` argument.
   */
  type Max = Max.type

  /**
   * The `MIN` argument of `ZMPOP` and `BZMPOP`, which pop the members with the lowest scores. Sent as `MIN`.
   */
  case object Min {
    private[redis] def asString: String = "MIN"
  }

  /**
   * The type of the `Min` argument.
   */
  type Min = Min.type

  /**
   * The `CH` flag for `ZADD`. The reply counts the members that changed as well as the members that were added.
   */
  case object Changed {
    private[redis] def asString: String = "CH"
  }

  /**
   * The type of the `Changed` flag.
   */
  type Changed = Changed.type

  /**
   * The `INCR` flag for `ZADD`. It adds the given score to the current score of the member, and Redis replies
   * with the new score.
   */
  case object Increment {
    private[redis] def asString: String = "INCR"
  }

  /**
   * The type of the `Increment` flag.
   */
  type Increment = Increment.type

  /**
   * A member of a sorted set and its score.
   */
  sealed case class MemberScore[+M](member: M, score: Double)

  /**
   * A chunk of members with their scores.
   */
  type MemberScores[+M] = Chunk[MemberScore[M]]

  /**
   * The rank and the score of a member, as returned by `ZRANK` and `ZREVRANK` with `WITHSCORE`. The rank
   * counts from 0.
   */
  sealed case class RankScore(rank: Long, score: Double)

  /**
   * The `LIMIT` argument of `ZINTERCARD`: the cardinality at which Redis stops counting the intersection.
   */
  sealed case class SimpleLimit(limit: Long)

  /**
   * The upper bound of a lexicographic range, used by `ZRANGE` with `BYLEX` and by `ZLEXCOUNT`.
   */
  sealed trait LexMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case LexMaximum.UnboundedPositive => "+"
        case LexMaximum.UnboundedNegative => "-"
        case LexMaximum.Open(value)       => s"($value"
        case LexMaximum.Closed(value)     => s"[$value"
      }
  }

  /**
   * The upper bounds of a lexicographic range.
   */
  object LexMaximum {

    /**
     * No upper bound. Sent as `+`.
     */
    case object UnboundedPositive extends LexMaximum

    /**
     * Sent as `-`. As an upper bound it selects no member: `ZLEXCOUNT` with `[a` and `-` returned 0 on Redis 7.2.
     */
    case object UnboundedNegative extends LexMaximum

    /**
     * Excludes `value` from the range. Sent as `(` followed by the value.
     */
    sealed case class Open(value: String) extends LexMaximum

    /**
     * Includes `value` in the range. Sent as `[` followed by the value.
     */
    sealed case class Closed(value: String) extends LexMaximum
  }

  /**
   * The lower bound of a lexicographic range, used by `ZRANGE` with `BYLEX` and by `ZLEXCOUNT`.
   */
  sealed trait LexMinimum { self =>
    private[redis] final def asString: String =
      self match {
        case LexMinimum.UnboundedPositive => "+"
        case LexMinimum.UnboundedNegative => "-"
        case LexMinimum.Open(value)       => s"($value"
        case LexMinimum.Closed(value)     => s"[$value"
      }
  }

  /**
   * The lower bounds of a lexicographic range.
   */
  object LexMinimum {

    /**
     * Sent as `+`. As a lower bound it selects no member: `ZLEXCOUNT` with `+` and `[z` returned 0 on Redis 7.2.
     */
    case object UnboundedPositive extends LexMinimum

    /**
     * No lower bound. Sent as `-`.
     */
    case object UnboundedNegative extends LexMinimum

    /**
     * Excludes `value` from the range. Sent as `(` followed by the value.
     */
    sealed case class Open(value: String) extends LexMinimum

    /**
     * Includes `value` in the range. Sent as `[` followed by the value.
     */
    sealed case class Closed(value: String) extends LexMinimum
  }

  /**
   * The upper bound of a score range, used in a `ScoreRange` for `ZRANGE` with `BYSCORE`.
   */
  sealed trait ScoreMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case ScoreMaximum.Infinity      => "+inf"
        case ScoreMaximum.Open(value)   => s"($value"
        case ScoreMaximum.Closed(value) => s"$value"
      }
  }

  /**
   * The upper bounds of a score range.
   */
  object ScoreMaximum {

    /**
     * No upper bound. Sent as `+inf`.
     */
    case object Infinity extends ScoreMaximum

    /**
     * Excludes scores equal to `value`. Sent as `(` followed by the value.
     */
    sealed case class Open(value: Double) extends ScoreMaximum

    /**
     * Includes scores equal to `value`. Sent as the number, so `1e20` is sent as `1.0E20`, which Redis accepts.
     */
    sealed case class Closed(value: Double) extends ScoreMaximum
  }

  /**
   * The lower bound of a score range, used in a `ScoreRange` for `ZRANGE` with `BYSCORE`.
   */
  sealed trait ScoreMinimum { self =>
    private[redis] final def asString: String =
      self match {
        case ScoreMinimum.Infinity      => "-inf"
        case ScoreMinimum.Open(value)   => s"($value"
        case ScoreMinimum.Closed(value) => s"$value"
      }
  }

  /**
   * The lower bounds of a score range.
   */
  object ScoreMinimum {

    /**
     * No lower bound. Sent as `-inf`.
     */
    case object Infinity extends ScoreMinimum

    /**
     * Excludes scores equal to `value`. Sent as `(` followed by the value.
     */
    sealed case class Open(value: Double) extends ScoreMinimum

    /**
     * Includes scores equal to `value`. Sent as the number, so `1e-5` is sent as `1.0E-5`, which Redis accepts.
     */
    sealed case class Closed(value: Double) extends ScoreMinimum
  }

  /**
   * The start index of an index range for `ZRANGE`. A negative value counts from the end of the set.
   */
  sealed case class RangeMinimum(value: Long) {
    private[redis] final def asString: String = s"$value"
  }

  /**
   * The end index of an index range for `ZRANGE`. The end is either included or excluded.
   */
  sealed trait RangeMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case RangeMaximum.Inclusive(value) => s"$value"
        case RangeMaximum.Exclusive(value) => s"${value - 1}"
      }
  }

  /**
   * The end indexes of an index range.
   */
  object RangeMaximum {

    /**
     * The end index is included. Sent as `value`, and a negative value counts from the end of the set.
     */
    sealed case class Inclusive(value: Long) extends RangeMaximum

    /**
     * The end index is excluded. The range stops at `value - 1`, which is the number that is sent. For `value`
     * 0 that number is `-1`, which Redis reads as the last element. `zRange` with `Exclusive(0)` therefore
     * returns the whole sorted set, and `zRange` with `0 until 0` does the same.
     */
    sealed case class Exclusive(value: Long) extends RangeMaximum
  }

  /**
   * A range for `ZRANGE`: an index range, a lexicographic range, or a score range.
   *
   * {{{
   * import zio._
   * import zio.redis._
   *
   * // The members with ranks 0 and 1 of the sorted set "scores"
   * val firstTwo: ZIO[Redis, RedisError, Chunk[String]] =
   *   ZIO.serviceWithZIO[Redis](
   *     _.zRange("scores", SortedSetRange.Range(RangeMinimum(0), RangeMaximum.Inclusive(1))).returning[String]
   *   )
   * }}}
   *
   * @see [[https://zio.dev/zio-redis/ ZIO Redis]]
   */
  sealed trait SortedSetRange

  /**
   * The three kinds of range for `ZRANGE`.
   */
  object SortedSetRange {

    /**
     * A lexicographic range. `ZRANGE` sends it with `BYLEX`, and with `rev` it swaps the bounds.
     */
    sealed case class LexRange(min: LexMinimum, max: LexMaximum) extends SortedSetRange

    /**
     * A score range. `ZRANGE` sends it with `BYSCORE`, and with `rev` it swaps the bounds.
     */
    sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum) extends SortedSetRange

    /**
     * An index range from `min` to `max`, counted by position. `ZRANGE` sends no `BYSCORE` or `BYLEX` flag for it.
     */
    sealed case class Range(min: RangeMinimum, max: RangeMaximum) extends SortedSetRange
  }

  /**
   * The `WITHSCORE` flag for `ZRANK` and `ZREVRANK`. It makes them return the score along with the rank.
   */
  case object WithScore {
    private[redis] def asString: String = "WITHSCORE"
  }

  /**
   * The type of the `WithScore` flag.
   */
  type WithScore = WithScore.type

  /**
   * The `WITHSCORES` flag. It makes the commands that return members also return their scores.
   */
  case object WithScores {
    private[redis] def asString: String = "WITHSCORES"
  }

  /**
   * The type of the `WithScores` flag.
   */
  type WithScores = WithScores.type

}
