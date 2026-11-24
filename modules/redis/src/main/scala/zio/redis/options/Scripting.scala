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
 * Types for the scripting commands: `SCRIPT DEBUG` and `SCRIPT FLUSH`.
 */
trait Scripting {
  /**
   * The debug mode for `SCRIPT DEBUG`.
   */
  sealed trait DebugMode { self =>
    private[redis] final def asString: String =
      self match {
        case DebugMode.Yes  => "YES"
        case DebugMode.Sync => "SYNC"
        case DebugMode.No   => "NO"
      }
  }

  /**
   * The debug modes for `SCRIPT DEBUG`.
   */
  object DebugMode {
    /**
     * Enables asynchronous debugging of Lua scripts. Sent as `YES`.
     */
    case object Yes  extends DebugMode
    /**
     * Enables synchronous debugging of Lua scripts. Sent as `SYNC`.
     */
    case object Sync extends DebugMode
    /**
     * Turns Lua script debugging off. Sent as `NO`.
     */
    case object No   extends DebugMode
  }

  /**
   * The mode for `SCRIPT FLUSH`, which empties the script cache. When no mode is given, the command sends no
   * argument and Redis uses its default mode.
   */
  sealed trait FlushMode { self =>
    private[redis] final def asString: String =
      self match {
        case FlushMode.Async => "ASYNC"
        case FlushMode.Sync  => "SYNC"
      }
  }

  /**
   * The modes for `SCRIPT FLUSH`.
   */
  object FlushMode {
    /**
     * Flushes the script cache asynchronously. Sent as `ASYNC`.
     */
    case object Async extends FlushMode
    /**
     * Flushes the script cache synchronously. Sent as `SYNC`.
     */
    case object Sync  extends FlushMode
  }
}
