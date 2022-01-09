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

trait Shared {

  sealed trait Update { self =>
    private[redis] final def stringify: String =
      self match {
        case Update.SetExisting    => "XX"
        case Update.SetNew         => "NX"
        case Update.SetLessThan    => "LT"
        case Update.SetGreaterThan => "GT"
      }
  }

  object Update {
    case object SetExisting    extends Update
    case object SetNew         extends Update
    case object SetLessThan    extends Update
    case object SetGreaterThan extends Update
  }

  sealed case class Count(count: Long)

  sealed trait Order { self =>
    private[redis] final def stringify: String =
      self match {
        case Order.Ascending  => "ASC"
        case Order.Descending => "DESC"
      }
  }

  object Order {
    case object Ascending  extends Order
    case object Descending extends Order
  }

  sealed case class Limit(offset: Long, count: Long)

  sealed case class Store(key: String)

  sealed case class Pattern(pattern: String)
}
