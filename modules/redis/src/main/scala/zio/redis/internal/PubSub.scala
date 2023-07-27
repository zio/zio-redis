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

package zio.redis.internal

private[redis] object PubSub {
  private[redis] sealed trait PushMessage {
    def key: SubscriptionKey
  }

  private[redis] object PushMessage {
    final case class Subscribed(key: SubscriptionKey, numOfSubs: Long)                      extends PushMessage
    final case class Unsubscribed(key: SubscriptionKey, numOfSubs: Long)                    extends PushMessage
    final case class Message(key: SubscriptionKey, destChannel: String, message: RespValue) extends PushMessage
  }

  private[redis] sealed trait SubscriptionKey {
    def value: String
  }

  private[redis] object SubscriptionKey {
    final case class Channel(value: String) extends SubscriptionKey
    final case class Pattern(value: String) extends SubscriptionKey
  }
}
