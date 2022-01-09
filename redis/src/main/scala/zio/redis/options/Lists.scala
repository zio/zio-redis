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

trait Lists {
  sealed trait Position { self =>
    private[redis] final def stringify: String =
      self match {
        case Position.Before => "BEFORE"
        case Position.After  => "AFTER"
      }
  }

  object Position {
    case object Before extends Position
    case object After  extends Position
  }

  sealed trait Side { self =>
    private[redis] final def stringify: String =
      self match {
        case Side.Left  => "LEFT"
        case Side.Right => "RIGHT"
      }
  }

  object Side {
    case object Left  extends Side
    case object Right extends Side
  }

  sealed case class ListMaxLen(count: Long)

  sealed case class Rank(rank: Long)
}
