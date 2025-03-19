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

trait SortedSets {
  sealed trait Aggregate extends Product { self =>
    private[redis] final def asString: String =
      self match {
        case Aggregate.Max => "MAX"
        case Aggregate.Min => "MIN"
        case Aggregate.Sum => "SUM"
      }
  }

  object Aggregate {
    case object Sum extends Aggregate
    case object Min extends Aggregate
    case object Max extends Aggregate
  }

  case object Max {
    private[redis] def asSring: String = "MAX"
  }

  type Max = Max.type

  case object Min {
    private[redis] def asString: String = "MIN"
  }

  type Min = Min.type

  case object Changed {
    private[redis] def asString: String = "CH"
  }

  type Changed = Changed.type

  case object Increment {
    private[redis] def asString: String = "INCR"
  }

  type Increment = Increment.type

  sealed case class MemberScore[+M](member: M, score: Double)

  type MemberScores[+M] = Chunk[MemberScore[M]]

  sealed case class RankScore(rank: Long, score: Double)

  sealed case class SimpleLimit(limit: Long)

  sealed trait LexMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case LexMaximum.UnboundedPositive => "+"
        case LexMaximum.UnboundedNegative => "-"
        case LexMaximum.Open(value)       => s"($value"
        case LexMaximum.Closed(value)     => s"[$value"
      }
  }

  object LexMaximum {
    case object UnboundedPositive           extends LexMaximum
    case object UnboundedNegative           extends LexMaximum
    sealed case class Open(value: String)   extends LexMaximum
    sealed case class Closed(value: String) extends LexMaximum
  }

  sealed trait LexMinimum { self =>
    private[redis] final def asString: String =
      self match {
        case LexMinimum.UnboundedPositive => "+"
        case LexMinimum.UnboundedNegative => "-"
        case LexMinimum.Open(value)       => s"($value"
        case LexMinimum.Closed(value)     => s"[$value"
      }
  }

  object LexMinimum {
    case object UnboundedPositive           extends LexMinimum
    case object UnboundedNegative           extends LexMinimum
    sealed case class Open(value: String)   extends LexMinimum
    sealed case class Closed(value: String) extends LexMinimum
  }

  sealed trait ScoreMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case ScoreMaximum.Infinity      => "+inf"
        case ScoreMaximum.Open(value)   => s"($value"
        case ScoreMaximum.Closed(value) => s"$value"
      }
  }

  object ScoreMaximum {
    case object Infinity                    extends ScoreMaximum
    sealed case class Open(value: Double)   extends ScoreMaximum
    sealed case class Closed(value: Double) extends ScoreMaximum
  }

  sealed trait ScoreMinimum { self =>
    private[redis] final def asString: String =
      self match {
        case ScoreMinimum.Infinity      => "-inf"
        case ScoreMinimum.Open(value)   => s"($value"
        case ScoreMinimum.Closed(value) => s"$value"
      }
  }

  object ScoreMinimum {
    case object Infinity                    extends ScoreMinimum
    sealed case class Open(value: Double)   extends ScoreMinimum
    sealed case class Closed(value: Double) extends ScoreMinimum
  }

  sealed case class RangeMinimum(value: Long) {
    private[redis] final def asString: String = s"$value"
  }

  sealed trait RangeMaximum { self =>
    private[redis] final def asString: String =
      self match {
        case RangeMaximum.Inclusive(value) => s"$value"
        case RangeMaximum.Exclusive(value) => s"${value - 1}"
      }
  }

  object RangeMaximum {
    sealed case class Inclusive(value: Long) extends RangeMaximum
    sealed case class Exclusive(value: Long) extends RangeMaximum
  }

  sealed trait SortedSetRange

  object SortedSetRange {
    sealed case class LexRange(min: LexMinimum, max: LexMaximum)       extends SortedSetRange
    sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum) extends SortedSetRange
    sealed case class Range(min: RangeMinimum, max: RangeMaximum)      extends SortedSetRange
  }

  case object WithScore {
    private[redis] def asString: String = "WITHSCORE"
  }

  type WithScore = WithScore.type

  case object WithScores {
    private[redis] def asString: String = "WITHSCORES"
  }

  type WithScores = WithScores.type

}
