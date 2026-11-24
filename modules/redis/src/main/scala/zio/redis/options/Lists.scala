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

/**
 * Types for the list commands.
 */
trait Lists {
  /**
   * Where `LINSERT` puts the new element, relative to the pivot element.
   */
  sealed trait Position { self =>
    private[redis] final def asString: String =
      self match {
        case Position.Before => "BEFORE"
        case Position.After  => "AFTER"
      }
  }

  /**
   * The positions for `LINSERT`.
   */
  object Position {
    /**
     * Inserts the element before the pivot. Sent as `BEFORE`.
     */
    case object Before extends Position
    /**
     * Inserts the element after the pivot. Sent as `AFTER`.
     */
    case object After  extends Position
  }

  /**
   * One end of a list: the head (`LEFT`) or the tail (`RIGHT`). `LMOVE` and `BLMOVE` take one side for the
   * source list and one for the destination list.
   */
  sealed trait Side { self =>
    private[redis] final def asString: String =
      self match {
        case Side.Left  => "LEFT"
        case Side.Right => "RIGHT"
      }
  }

  /**
   * The two ends of a list.
   */
  object Side {
    /**
     * The head of the list. Sent as `LEFT`.
     */
    case object Left  extends Side
    /**
     * The tail of the list. Sent as `RIGHT`.
     */
    case object Right extends Side
  }

  /**
   * The `MAXLEN` option for `LPOS`, which limits how many list elements Redis compares. Sent as
   * `MAXLEN count`.
   */
  sealed case class ListMaxLen(count: Long)

  /**
   * The `RANK` option for `LPOS`: which match to return. 1 is the first match, 2 the second, and a negative
   * number counts from the end of the list. Redis rejects 0 with the error
   * `RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start
   * from the end of the list`.
   */
  sealed case class Rank(rank: Long)
}
