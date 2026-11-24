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

import zio.redis.internal.PubSub.SubscriptionKey
import zio.redis.internal.RespCommand
import zio.redis.options.Cluster.Slot

import java.io.IOException
import scala.util.control.NoStackTrace

/**
 * The base type of the errors that Redis commands and the client return. Every case extends `NoStackTrace`, so an error
 * is created without a stack trace.
 *
 * {{{
 * def describe(error: RedisError): String =
 *   error match {
 *     case RedisError.WrongType(message) => "Wrong type: " + message
 *     case other                         => other.toString
 *   }
 * }}}
 *
 * @see [[https://zio.dev/zio-redis/error-handling/ Error handling]]
 */
sealed trait RedisError extends NoStackTrace

/**
 * The cases of `RedisError`. Each case is returned by a Redis command, or raised by the client.
 */
object RedisError {

  /**
   * Errors that a Redis cluster returns for a command. The cluster client handles each one by redirecting the command
   * (`Ask` and `Moved`) or by retrying it (`CrossSlot`, and a redirect that fails with an `IOError`).
   */
  sealed trait ClusterRedisError extends RedisError

  /**
   * The server replied with an error that has no more specific case, or a reply could not be decoded. For an error
   * reply that starts with `ERR` or `NOAUTH`, the message is the text after that word, trimmed. Other error replies
   * keep their whole text. Bytes that are not valid RESP give the message `Invalid data received.`; a reply of the
   * wrong shape for the command gives a message such as `... isn't a bulk string`.
   */
  final case class ProtocolError(message: String) extends RedisError {
    override def toString: String = s"ProtocolError: $message"
  }

  /**
   * A value could not be converted to bytes or back by the codec. For example, `CodecSupplier.utf8` fails with the
   * message `the codec support only primitives` when asked to convert a list or a record.
   */
  final case class CodecError(message: String) extends RedisError {
    override def toString: String = s"CodecError: $message"
  }

  /**
   * The key holds a value of another type than the command expects (`WRONGTYPE`). The message is the reply text after
   * the prefix, for example `Operation against a key holding the wrong kind of value`.
   */
  final case class WrongType(message: String) extends RedisError

  /**
   * A consumer group with this name already exists (`BUSYGROUP`), for example after `XGROUP CREATE` on an existing
   * group. The message is the reply text after the prefix.
   */
  final case class BusyGroup(message: String) extends RedisError

  /**
   * The stream or the consumer group does not exist (`NOGROUP`), for example after `XREADGROUP` with an unknown group.
   * The message is the reply text after the prefix.
   */
  final case class NoGroup(message: String) extends RedisError

  /**
   * The script with the given SHA1 is not in the script cache (`NOSCRIPT`), for example after `EVALSHA` with a SHA1
   * that was never loaded. The message is the reply text after the prefix.
   */
  final case class NoScript(message: String) extends RedisError

  /**
   * No script is running, so `SCRIPT KILL` has nothing to stop (`NOTBUSY`). The message is the reply text after the
   * prefix.
   */
  final case class NotBusy(message: String) extends RedisError

  /**
   * The keys of one command are in different hash slots of a cluster (`CROSSSLOT`). The cluster client retries the
   * command with the backoff of its `RetryClusterConfig` before it returns this error.
   */
  final case class CrossSlot(message: String) extends ClusterRedisError

  /**
   * The slot is being moved to another node (`ASK`). The cluster client sends `ASKING` to `address`, then runs the
   * command on that node.
   */
  final case class Ask(slot: Slot, address: RedisUri) extends ClusterRedisError

  /**
   * Creates an `Ask` from a pair of slot and address.
   */
  object Ask {

    /**
     * Creates an `Ask` from a pair of slot and address.
     */
    def apply(slotAndAddress: (Slot, RedisUri)): Ask = Ask(slotAndAddress._1, slotAndAddress._2)
  }

  /**
   * The slot is served by another node (`MOVED`). The cluster client refreshes its view of the cluster and runs the
   * command again. It does not send the retry to `address` directly.
   */
  final case class Moved(slot: Slot, address: RedisUri) extends ClusterRedisError

  /**
   * Creates a `Moved` from a pair of slot and address.
   */
  object Moved {

    /**
     * Creates a `Moved` from a pair of slot and address.
     */
    def apply(slotAndAddress: (Slot, RedisUri)): Moved = Moved(slotAndAddress._1, slotAndAddress._2)
  }

  /**
   * The connection to the server failed or was lost. Wraps the `IOException`, for example the refusal of a connection
   * when no server listens on the port.
   */
  final case class IOError(exception: IOException) extends RedisError

  /**
   * The client found no command name in the arguments of a subscription command. When the error is created with the
   * companion `apply`, the message is the string form of the command's arguments.
   */
  final case class CommandNameNotFound(message: String) extends RedisError

  /**
   * Creates `CommandNameNotFound` errors from commands.
   */
  object CommandNameNotFound {

    /**
     * Creates the error from a command, with the string form of its arguments as the message.
     */
    def apply(command: RespCommand): CommandNameNotFound = CommandNameNotFound(command.args.toString())
  }

  /**
   * An error of the publish and subscribe commands.
   */
  sealed trait PubSubError extends RedisError

  /**
   * A command name that the subscription client does not handle. The subscribe and unsubscribe methods send only
   * `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE` and `PUNSUBSCRIBE`, so this error is not expected from them.
   */
  final case class InvalidPubSubCommand(command: String) extends PubSubError

  /**
   * A subscription confirmation arrived for a channel or pattern that had already been unsubscribed, so the client
   * found no stream to deliver its messages to.
   */
  final case class SubscriptionStreamAlreadyClosed(key: SubscriptionKey) extends PubSubError
}
