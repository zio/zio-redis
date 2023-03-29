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

trait Keys {

  case object AbsTtl {
    private[redis] def asString: String = "ABSTTL"
  }

  type AbsTtl = AbsTtl.type

  case object Alpha {
    private[redis] def asString: String = "ALPHA"
  }

  type Alpha = Alpha.type

  sealed case class Auth(username: Option[String], password: String)

  case object Copy {
    private[redis] def asString: String = "COPY"
  }

  type Copy = Copy.type

  sealed case class IdleTime(seconds: Long)

  sealed case class Freq(frequency: String)

  sealed trait RedisType extends Product with Serializable { self =>
    private[redis] final def asString: String =
      self match {
        case RedisType.String    => "string"
        case RedisType.List      => "list"
        case RedisType.Set       => "set"
        case RedisType.SortedSet => "zset"
        case RedisType.Hash      => "hash"
        case RedisType.Stream    => "stream"
      }
  }

  object RedisType {
    case object String    extends RedisType
    case object List      extends RedisType
    case object Set       extends RedisType
    case object SortedSet extends RedisType
    case object Hash      extends RedisType
    case object Stream    extends RedisType
  }

  case object Replace {
    private[redis] def asString: String = "REPLACE"
  }

  type Replace = Replace.type
}
