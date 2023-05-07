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

import zio.UIO
import zio.redis.internal.RespValue

object PubSub {
  type PubSubCallback = (String, Long) => UIO[Unit]

  private[redis] sealed trait PushProtocol

  private[redis] object PushProtocol {
    case class Subscribe(channel: String, numOfSubs: Long)                    extends PushProtocol
    case class PSubscribe(pattern: String, numOfSubs: Long)                   extends PushProtocol
    case class Unsubscribe(channel: String, numOfSubs: Long)                  extends PushProtocol
    case class PUnsubscribe(pattern: String, numOfSubs: Long)                 extends PushProtocol
    case class Message(channel: String, message: RespValue)                   extends PushProtocol
    case class PMessage(pattern: String, channel: String, message: RespValue) extends PushProtocol
  }

  final case class NumberOfSubscribers(channel: String, subscriberCount: Long)
}
