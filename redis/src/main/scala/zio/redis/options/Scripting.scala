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

trait Scripting {
  sealed trait DebugMode { self =>
    private[redis] final def stringify: String =
      self match {
        case DebugMode.Yes  => "YES"
        case DebugMode.Sync => "SYNC"
        case DebugMode.No   => "NO"
      }
  }

  object DebugMode {
    case object Yes  extends DebugMode
    case object Sync extends DebugMode
    case object No   extends DebugMode
  }

  sealed trait FlushMode { self =>
    private[redis] final def stringify: String =
      self match {
        case FlushMode.Async => "ASYNC"
        case FlushMode.Sync  => "SYNC"
      }
  }

  object FlushMode {
    case object Async extends FlushMode
    case object Sync  extends FlushMode
  }
}
