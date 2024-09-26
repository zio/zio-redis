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

import java.time.Instant

trait Strings {

  sealed trait Lcs

  object Lcs {
    case class PlainLcs(lcs: String)                       extends Lcs
    case class Length(length: Long)                        extends Lcs
    case class Matches(matches: List[Match], length: Long) extends Lcs
  }

  sealed trait LcsQueryType

  object LcsQueryType {
    case object Len                                                           extends LcsQueryType
    case class Idx(minMatchLength: Int = 1, withMatchLength: Boolean = false) extends LcsQueryType
  }

  case class MatchIdx(start: Long, end: Long)
  case class Match(matchIdxA: MatchIdx, matchIdxB: MatchIdx, matchLength: Option[Long] = None)

  sealed trait BitFieldCommand

  object BitFieldCommand {
    sealed case class BitFieldGet(`type`: BitFieldType, offset: Int) extends BitFieldCommand

    sealed case class BitFieldSet(`type`: BitFieldType, offset: Int, value: Long) extends BitFieldCommand

    sealed case class BitFieldIncr(`type`: BitFieldType, offset: Int, increment: Long) extends BitFieldCommand

    sealed trait BitFieldOverflow extends BitFieldCommand { self =>
      private[redis] final def asString: String =
        self match {
          case BitFieldOverflow.Fail => "FAIL"
          case BitFieldOverflow.Sat  => "SAT"
          case BitFieldOverflow.Wrap => "WRAP"
        }
    }

    object BitFieldOverflow {
      case object Fail extends BitFieldOverflow
      case object Sat  extends BitFieldOverflow
      case object Wrap extends BitFieldOverflow
    }
  }

  sealed trait BitFieldType { self =>
    private[redis] final def asString: String =
      self match {
        case BitFieldType.UnsignedInt(size) => s"u$size"
        case BitFieldType.SignedInt(size)   => s"i$size"
      }
  }

  object BitFieldType {
    sealed case class UnsignedInt(size: Int) extends BitFieldType
    sealed case class SignedInt(size: Int)   extends BitFieldType
  }

  sealed trait BitOperation { self =>
    private[redis] final def asString: String =
      self match {
        case BitOperation.AND => "AND"
        case BitOperation.OR  => "OR"
        case BitOperation.XOR => "XOR"
        case BitOperation.NOT => "NOT"
      }
  }

  object BitOperation {
    case object AND extends BitOperation
    case object OR  extends BitOperation
    case object XOR extends BitOperation
    case object NOT extends BitOperation
  }

  sealed case class BitPosRange(start: Long, end: Option[Long])

  sealed trait SetExpire

  object SetExpire {
    case object KeepTtl                                       extends SetExpire
    final case class Milliseconds(milliseconds: Long)         extends SetExpire
    final case class Seconds(seconds: Long)                   extends SetExpire
    final case class UnixTimeMilliseconds(milliseconds: Long) extends SetExpire
    final case class UnixTimeSeconds(seconds: Instant)        extends SetExpire
  }

  sealed trait GetExpire

  object GetExpire {
    final case class Milliseconds(milliseconds: Long)         extends GetExpire
    case object Persist                                       extends GetExpire
    final case class Seconds(seconds: Long)                   extends GetExpire
    final case class UnixTimeMilliseconds(milliseconds: Long) extends GetExpire
    final case class UnixTimeSeconds(seconds: Long)           extends GetExpire
  }

  case object GetKeyword {
    private[redis] def asString: String = "GET"
  }

  type GetKeyword = GetKeyword.type
}
