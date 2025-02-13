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

import zio._

trait Streams {
  case object WithForce {
    private[redis] def asString = "FORCE"
  }

  type WithForce = WithForce.type

  case object WithJustId {
    private[redis] def asString = "JUSTID"
  }

  type WithJustId = WithJustId.type

  sealed case class LastId[I](lastId: I)

  sealed case class WithEntriesRead(entries: Long)

  sealed trait XGroupCommand

  object XGroupCommand {
    sealed case class Create[SK, SG, I](key: SK, group: SG, id: I)                 extends XGroupCommand
    sealed case class SetId[SK, SG, I](key: SK, group: SG, id: I)                  extends XGroupCommand
    sealed case class Destroy[SK, SG](key: SK, group: SG)                          extends XGroupCommand
    sealed case class CreateConsumer[SK, SG, SC](key: SK, group: SG, consumer: SC) extends XGroupCommand
    sealed case class DelConsumer[SK, SG, SC](key: SK, group: SG, consumer: SC)    extends XGroupCommand
  }

  case object MkStream {
    private[redis] def asString = "MKSTREAM"
  }

  type MkStream = MkStream.type

  case object NoMkStream {
    private[redis] def asString = "NOMKSTREAM"
  }

  type NoMkStream = NoMkStream.type

  sealed case class PendingInfo(
    total: Long,
    first: Option[String],
    last: Option[String],
    consumers: Map[String, Long]
  )

  sealed case class PendingMessage(
    id: String,
    owner: String,
    lastDelivered: Duration,
    counter: Long
  )

  case object NoAck {
    private[redis] def asString: String = "NOACK"
  }

  type NoAck = NoAck.type

  type StreamEntries[I, K, V] = Chunk[StreamEntry[I, K, V]]

  type StreamChunks[N, I, K, V] = Chunk[StreamChunk[N, I, K, V]]

  sealed trait CappedStreamType

  object CappedStreamType {
    sealed case class MaxLenApprox(count: Long, limit: Option[Long]) extends CappedStreamType
    sealed case class MaxLenExact(count: Long)                       extends CappedStreamType
    sealed case class MinIdApprox[I](id: I, limit: Option[Long])     extends CappedStreamType
    sealed case class MinIdExact[I](id: I)                           extends CappedStreamType
  }

  sealed case class StreamEntry[I, K, V](id: I, fields: Map[K, V])

  sealed case class StreamChunk[N, I, K, V](name: N, entries: Chunk[StreamEntry[I, K, V]])

  sealed case class StreamClaimedEntries[SI, I, K, V](
    streamId: SI,
    entries: Chunk[StreamEntry[I, K, V]],
    deletedIds: Chunk[I]
  )

  sealed case class StreamClaimedIdChunk[SI, I](streamId: SI, claimedIds: Chunk[I], deletedIds: Chunk[I])

  sealed case class StreamInfo[I, K, V](
    length: Long,
    radixTreeKeys: Long,
    radixTreeNodes: Long,
    groups: Long,
    lastGeneratedId: String,
    firstEntry: Option[StreamEntry[I, K, V]],
    lastEntry: Option[StreamEntry[I, K, V]]
  )

  object StreamInfo {
    def empty[I, K, V]: StreamInfo[I, K, V] = StreamInfo(0, 0, 0, 0, "", None, None)
  }

  sealed case class StreamGroupsInfo(
    name: String,
    consumers: Long,
    pending: Long,
    lastDeliveredId: String
  )

  object StreamGroupsInfo {
    def empty: StreamGroupsInfo = StreamGroupsInfo("", 0, 0, "")
  }

  sealed case class StreamConsumersInfo(
    name: String,
    pending: Long,
    idle: Duration,
    inactive: Duration
  )

  object StreamConsumersInfo {
    def empty: StreamConsumersInfo = StreamConsumersInfo("", 0, 0.millis, 0.millis)
  }

  object StreamInfoWithFull {

    sealed case class FullStreamInfo[I, K, V](
      length: Long,
      radixTreeKeys: Long,
      radixTreeNodes: Long,
      lastGeneratedId: String,
      entries: Chunk[StreamEntry[I, K, V]],
      groups: Chunk[ConsumerGroups]
    )

    object FullStreamInfo {
      def empty[I, K, V]: FullStreamInfo[I, K, V] = FullStreamInfo(0, 0, 0, "", Chunk.empty, Chunk.empty)
    }

    sealed case class ConsumerGroups(
      name: String,
      lastDeliveredId: String,
      pelCount: Long,
      pending: Chunk[GroupPel],
      consumers: Chunk[Consumers]
    )

    object ConsumerGroups {
      def empty: ConsumerGroups = ConsumerGroups("", "", 0, Chunk.empty, Chunk.empty)
    }

    sealed case class GroupPel(entryId: String, consumerName: String, deliveryTime: Duration, deliveryCount: Long)

    sealed case class Consumers(name: String, seenTime: Duration, pelCount: Long, pending: Chunk[ConsumerPel])

    object Consumers {
      def empty: Consumers = Consumers("", 0.millis, 0, Chunk.empty)
    }

    sealed case class ConsumerPel(entryId: String, deliveryTime: Duration, deliveryCount: Long)
  }

  private[redis] object XInfoFields {
    val Name: String    = "name"
    val Idle: String    = "idle"
    val Pending: String = "pending"
    val Inactive: String = "inactive"

    val Consumers: String       = "consumers"
    val LastDeliveredId: String = "last-delivered-id"

    val Length: String          = "length"
    val RadixTreeKeys: String   = "radix-tree-keys"
    val RadixTreeNodes: String  = "radix-tree-nodes"
    val Groups: String          = "groups"
    val LastGeneratedId: String = "last-generated-id"
    val FirstEntry: String      = "first-entry"
    val LastEntry: String       = "last-entry"

    val Entries: String  = "entries"
    val PelCount: String = "pel-count"
    val SeenTime: String = "seen-time"
  }
}
