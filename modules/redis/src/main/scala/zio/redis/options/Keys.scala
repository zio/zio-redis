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
 * Types for the key commands: `COPY`, `MIGRATE`, `RESTORE`, `SCAN`, `SORT` and `TYPE`.
 */
trait Keys {

  /**
   * The `ABSTTL` flag for `RESTORE`. With it, the `ttl` argument is an absolute Unix time in milliseconds,
   * not a duration.
   */
  case object AbsTtl {
    private[redis] def asString: String = "ABSTTL"
  }

  /**
   * The type of the `AbsTtl` flag.
   */
  type AbsTtl = AbsTtl.type

  /**
   * The `ALPHA` flag for `SORT`. It compares the values as strings instead of as floating point numbers.
   */
  case object Alpha {
    private[redis] def asString: String = "ALPHA"
  }

  /**
   * The type of the `Alpha` flag.
   */
  type Alpha = Alpha.type

  /**
   * Credentials for a Redis server. `Connection.auth` sends them with the `AUTH` command. A username and a
   * password are sent as two arguments, and a password alone as one.
   *
   * Passed as `auth` to `migrate`, the value is sent without the `AUTH` or `AUTH2` keyword. Redis 7.2 then
   * rejects the command with `ERR syntax error`.
   */
  sealed case class Auth(username: Option[String], password: String)

  /**
   * The `COPY` flag for `MIGRATE`. It keeps the key on the source server after the transfer.
   */
  case object Copy {
    private[redis] def asString: String = "COPY"
  }

  /**
   * The type of the `Copy` flag.
   */
  type Copy = Copy.type

  /**
   * The `IDLETIME` option for `RESTORE`. It sets the idle time of the restored key, in seconds.
   */
  sealed case class IdleTime(seconds: Long)

  /**
   * The `FREQ` option for `RESTORE`. It sets the access frequency counter of the restored key. The string is
   * sent as given, and the client does not check it.
   */
  sealed case class Freq(frequency: String)

  /**
   * The data type of a key, as reported by `TYPE`. It is also the filter of `SCAN`.
   *
   * `TYPE` answers `none` for a key that does not exist. This type has no case for `none`, so `typeOf`
   * fails for such a key with a `ProtocolError` whose message is `SimpleString(none) isn't redis type.`.
   */
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

  /**
   * The Redis data types. Each one sends the name that `TYPE` uses for it.
   */
  object RedisType {

    /**
     * A string value, reported as `string`.
     */
    case object String extends RedisType

    /**
     * A list value, reported as `list`.
     */
    case object List extends RedisType

    /**
     * A set value, reported as `set`.
     */
    case object Set extends RedisType

    /**
     * A sorted set value, reported as `zset`.
     */
    case object SortedSet extends RedisType

    /**
     * A hash value, reported as `hash`.
     */
    case object Hash extends RedisType

    /**
     * A stream value, reported as `stream`.
     */
    case object Stream extends RedisType
  }

  /**
   * The `REPLACE` flag. It overwrites an existing key. `COPY`, `MIGRATE` and `RESTORE` accept it.
   */
  case object Replace {
    private[redis] def asString: String = "REPLACE"
  }

  /**
   * The type of the `Replace` flag.
   */
  type Replace = Replace.type
}
