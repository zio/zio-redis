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

package zio.redis

import zio.redis.options.Cluster.Slot

import java.io.IOException
import scala.util.control.NoStackTrace

sealed trait RedisError extends NoStackTrace

object RedisError {
  sealed trait ClusterRedisError extends RedisError

  final case class ProtocolError(message: String) extends RedisError {
    override def toString: String = s"ProtocolError: $message"
  }
  final case class CodecError(message: String) extends RedisError {
    override def toString: String = s"CodecError: $message"
  }
  final case class WrongType(message: String)         extends RedisError
  final case class BusyGroup(message: String)         extends RedisError
  final case class NoGroup(message: String)           extends RedisError
  final case class NoScript(message: String)          extends RedisError
  final case class NotBusy(message: String)           extends RedisError
  final case class CrossSlot(message: String)         extends ClusterRedisError
  final case class Ask(slot: Slot, address: RedisUri) extends ClusterRedisError
  object Ask {
    def apply(slotAndAddress: (Slot, RedisUri)): Ask = Ask(slotAndAddress._1, slotAndAddress._2)
  }
  final case class Moved(slot: Slot, address: RedisUri) extends ClusterRedisError
  object Moved {
    def apply(slotAndAddress: (Slot, RedisUri)): Moved = Moved(slotAndAddress._1, slotAndAddress._2)
  }
  final case class IOError(exception: IOException) extends RedisError
  final case class CommandNameNotFound(message: String) extends RedisError
  sealed trait PubSubError extends RedisError
  final case class InvalidPubSubCommand(command: String) extends PubSubError
}
