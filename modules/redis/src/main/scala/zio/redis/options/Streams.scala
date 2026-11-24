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

/**
 * Types and flags for the stream commands, the `X` commands.
 */
trait Streams {

  /**
   * The `FORCE` flag for `XCLAIM`. Redis adds an entry that exists in the stream to the pending list of the
   * claiming consumer, even when no consumer had it pending.
   */
  case object WithForce {
    private[redis] def asString = "FORCE"
  }

  /**
   * The type of the `WithForce` flag.
   */
  type WithForce = WithForce.type

  /**
   * The `JUSTID` flag for `XCLAIM`. It returns only the IDs of the claimed entries, without their fields.
   */
  case object WithJustId {
    private[redis] def asString = "JUSTID"
  }

  /**
   * The type of the `WithJustId` flag.
   */
  type WithJustId = WithJustId.type

  /**
   * The `LASTID` argument of `XCLAIM`, sent as `LASTID` followed by the ID.
   */
  sealed case class LastId[I](lastId: I)

  /**
   * The `ENTRIESREAD` argument of `XGROUP CREATE` and `XGROUP SETID`, sent as `ENTRIESREAD` followed by the
   * number of entries the consumer group has read.
   */
  sealed case class WithEntriesRead(entries: Long)

  /**
   * A subcommand of `XGROUP`. Each case class holds the arguments of one subcommand.
   */
  sealed trait XGroupCommand

  /**
   * The subcommands of `XGROUP`.
   */
  object XGroupCommand {

    /**
     * `XGROUP CREATE`: creates a consumer group on the stream `key`. The group starts after the entry `id`.
     */
    sealed case class Create[SK, SG, I](key: SK, group: SG, id: I) extends XGroupCommand

    /**
     * `XGROUP SETID`: moves the last-delivered ID of the consumer group to `id`.
     */
    sealed case class SetId[SK, SG, I](key: SK, group: SG, id: I) extends XGroupCommand

    /**
     * `XGROUP DESTROY`: deletes the consumer group.
     */
    sealed case class Destroy[SK, SG](key: SK, group: SG) extends XGroupCommand

    /**
     * `XGROUP CREATECONSUMER`: adds a consumer to the consumer group.
     */
    sealed case class CreateConsumer[SK, SG, SC](key: SK, group: SG, consumer: SC) extends XGroupCommand

    /**
     * `XGROUP DELCONSUMER`: removes a consumer from the consumer group.
     */
    sealed case class DelConsumer[SK, SG, SC](key: SK, group: SG, consumer: SC) extends XGroupCommand
  }

  /**
   * The `MKSTREAM` flag. It creates an empty stream when the key does not exist. `xGroupCreate` sends it when
   * its `mkStream` argument is `true`.
   */
  case object MkStream {
    private[redis] def asString = "MKSTREAM"
  }

  /**
   * The type of the `MkStream` flag.
   */
  type MkStream = MkStream.type

  /**
   * The `NOMKSTREAM` flag. `XADD` does not create the stream when the key does not exist. The `xAdd` method
   * returns `None` in that case.
   */
  case object NoMkStream {
    private[redis] def asString = "NOMKSTREAM"
  }

  /**
   * The type of the `NoMkStream` flag.
   */
  type NoMkStream = NoMkStream.type

  /**
   * The summary of `XPENDING` without a range: the number of pending entries, the IDs of the first and last
   * pending entries (`None` when there are none), and the number of pending entries of each consumer.
   */
  sealed case class PendingInfo(
    total: Long,
    first: Option[String],
    last: Option[String],
    consumers: Map[String, Long]
  )

  /**
   * One pending entry from `XPENDING` with a range: its ID, the consumer that owns it, how many times it was
   * delivered, and the time since its last delivery. Redis reports that time as an idle time in milliseconds,
   * and the `lastDelivered` field holds it as a `Duration`.
   */
  sealed case class PendingMessage(
    id: String,
    owner: String,
    lastDelivered: Duration,
    counter: Long
  )

  /**
   * The `NOACK` flag for `XREADGROUP`. The entries that it returns are not added to the pending list.
   */
  case object NoAck {
    private[redis] def asString: String = "NOACK"
  }

  /**
   * The type of the `NoAck` flag.
   */
  type NoAck = NoAck.type

  /**
   * A chunk of stream entries.
   */
  type StreamEntries[I, K, V] = Chunk[StreamEntry[I, K, V]]

  /**
   * A chunk with one `StreamChunk` for each stream in a read.
   */
  type StreamChunks[N, I, K, V] = Chunk[StreamChunk[N, I, K, V]]

  /**
   * How a stream is trimmed, by its length or by its minimum ID. The `Approx` cases use the `~` form and the
   * `Exact` cases the `=` form. `XADD` and `XTRIM` accept it.
   */
  sealed trait CappedStreamType

  /**
   * The trimming strategies for `XADD` and `XTRIM`.
   */
  object CappedStreamType {

    /**
     * Trims to about `count` entries, sent as `MAXLEN ~ count`. A `limit` adds `LIMIT limit`, which is sent only
     * with this approximate form.
     */
    sealed case class MaxLenApprox(count: Long, limit: Option[Long]) extends CappedStreamType

    /**
     * Trims to exactly `count` entries, sent as `MAXLEN = count`.
     */
    sealed case class MaxLenExact(count: Long) extends CappedStreamType

    /**
     * Removes entries with IDs lower than `id`, approximately, sent as `MINID ~ id`. A `limit` adds
     * `LIMIT limit`, which is sent only with this approximate form.
     */
    sealed case class MinIdApprox[I](id: I, limit: Option[Long]) extends CappedStreamType

    /**
     * Removes entries with IDs lower than `id`, sent as `MINID = id`.
     */
    sealed case class MinIdExact[I](id: I) extends CappedStreamType
  }

  /**
   * A stream entry: its ID and its field-value pairs.
   */
  sealed case class StreamEntry[I, K, V](id: I, fields: Map[K, V])

  /**
   * The entries of one stream in a read that covers several streams, with the stream's `name`.
   */
  sealed case class StreamChunk[N, I, K, V](name: N, entries: Chunk[StreamEntry[I, K, V]])

  /**
   * The result of `XAUTOCLAIM`: the cursor for the next call (the field is named `streamId`), the entries
   * that were claimed, and the IDs of the entries that were deleted from the stream.
   */
  sealed case class StreamClaimedEntries[SI, I, K, V](
    streamId: SI,
    entries: Chunk[StreamEntry[I, K, V]],
    deletedIds: Chunk[I]
  )

  /**
   * The result of `XAUTOCLAIM` with `JUSTID`: the cursor for the next call (the field is named `streamId`),
   * the IDs of the claimed entries, and the IDs of the entries that were deleted from the stream.
   */
  sealed case class StreamClaimedIdChunk[SI, I](streamId: SI, claimedIds: Chunk[I], deletedIds: Chunk[I])

  /**
   * The result of `XINFO STREAM`: the length of the stream, its radix tree and entry counters, its last
   * generated and last deleted IDs, its group count, and its first and last entries. An entry is `None` when
   * Redis returns no entry, as on an empty stream.
   */
  sealed case class StreamInfo[I, K, V](
    length: Long,
    radixTreeKeys: Long,
    radixTreeNodes: Long,
    lastGeneratedId: String,
    maxDeletedEntryId: String,
    entriesAdded: Long,
    recordedFirstEntryId: String,
    groups: Long,
    firstEntry: Option[StreamEntry[I, K, V]],
    lastEntry: Option[StreamEntry[I, K, V]]
  )

  /**
   * Companion object of `StreamInfo`.
   */
  object StreamInfo {

    /**
     * An empty `StreamInfo`: all counters are 0, all strings are empty, and both entries are `None`.
     */
    def empty[I, K, V]: StreamInfo[I, K, V] = StreamInfo(0, 0, 0, "", "", 0, "", 0, None, None)
  }

  /**
   * One consumer group from `XINFO GROUPS`: its name, its consumer count, its pending count, its last-delivered
   * ID, its entries-read counter, and its lag.
   */
  sealed case class StreamGroupsInfo(
    name: String,
    consumers: Long,
    pending: Long,
    lastDeliveredId: String,
    entriesRead: Long,
    lag: Long
  )

  /**
   * Companion object of `StreamGroupsInfo`.
   */
  object StreamGroupsInfo {

    /**
     * An empty `StreamGroupsInfo`: an empty name and last-delivered ID, and zero counts.
     */
    def empty: StreamGroupsInfo = StreamGroupsInfo("", 0, 0, "", 0, 0)
  }

  /**
   * One consumer from `XINFO CONSUMERS`: its name, its pending count, its idle time and its inactive time, as
   * `Duration`s.
   */
  sealed case class StreamConsumersInfo(
    name: String,
    pending: Long,
    idle: Duration,
    inactive: Duration
  )

  /**
   * Companion object of `StreamConsumersInfo`.
   */
  object StreamConsumersInfo {

    /**
     * An empty `StreamConsumersInfo`: an empty name, a zero pending count, and zero durations.
     */
    def empty: StreamConsumersInfo = StreamConsumersInfo("", 0, 0.millis, 0.millis)
  }

  /**
   * Result types for `XINFO STREAM FULL`.
   */
  object StreamInfoWithFull {

    /**
     * The result of `XINFO STREAM FULL`: the counters of the stream, its entries, and its consumer groups.
     */
    sealed case class FullStreamInfo[I, K, V](
      length: Long,
      radixTreeKeys: Long,
      radixTreeNodes: Long,
      lastGeneratedId: String,
      maxDeletedEntryId: String,
      entriesAdded: Long,
      recordedFirstEntryId: String,
      entries: Chunk[StreamEntry[I, K, V]],
      groups: Chunk[ConsumerGroups]
    )

    /**
     * Companion object of `FullStreamInfo`.
     */
    object FullStreamInfo {

      /**
       * An empty `FullStreamInfo`: zero counters, empty strings, and no entries or groups.
       */
      def empty[I, K, V]: FullStreamInfo[I, K, V] = FullStreamInfo(0, 0, 0, "", "", 0, "", Chunk.empty, Chunk.empty)
    }

    /**
     * A consumer group in an `XINFO STREAM FULL` result, with its pending entries and its consumers.
     */
    sealed case class ConsumerGroups(
      name: String,
      lastDeliveredId: String,
      entriesRead: Long,
      lag: Long,
      pelCount: Long,
      pending: Chunk[GroupPel],
      consumers: Chunk[Consumers]
    )

    /**
     * Companion object of `ConsumerGroups`.
     */
    object ConsumerGroups {

      /**
       * An empty `ConsumerGroups`: an empty name, zero counts, and no pending entries or consumers.
       */
      def empty: ConsumerGroups = ConsumerGroups("", "", 0, 0, 0, Chunk.empty, Chunk.empty)
    }

    /**
     * A pending entry of a consumer group in an `XINFO STREAM FULL` result. `deliveryTime` is the Unix time of the
     * last delivery in milliseconds. It is stored in a `Duration` measured from zero, so use `toMillis` to read
     * it. It is not an elapsed time.
     */
    sealed case class GroupPel(entryId: String, consumerName: String, deliveryTime: Duration, deliveryCount: Long)

    /**
     * A consumer in an `XINFO STREAM FULL` result, with its pending entries. `seenTime` and `activeTime` are Unix
     * times in milliseconds, stored in `Duration`s measured from zero, as `deliveryTime` is in `GroupPel`.
     */
    sealed case class Consumers(
      name: String,
      seenTime: Duration,
      activeTime: Duration,
      pelCount: Long,
      pending: Chunk[ConsumerPel]
    )

    /**
     * Companion object of `Consumers`.
     */
    object Consumers {

      /**
       * An empty `Consumers`: an empty name, zero durations and counts, and no pending entries.
       */
      def empty: Consumers = Consumers("", 0.millis, 0.millis, 0, Chunk.empty)
    }

    /**
     * A pending entry of one consumer in an `XINFO STREAM FULL` result. `deliveryTime` has the same meaning as in
     * `GroupPel`.
     */
    sealed case class ConsumerPel(entryId: String, deliveryTime: Duration, deliveryCount: Long)
  }

  private[redis] object XInfoFields {
    val Name: String     = "name"
    val Idle: String     = "idle"
    val Pending: String  = "pending"
    val Inactive: String = "inactive"

    val Consumers: String       = "consumers"
    val LastDeliveredId: String = "last-delivered-id"
    val EntriesRead: String     = "entries-read"
    val Lag: String             = "lag"

    val Length: String          = "length"
    val RadixTreeKeys: String   = "radix-tree-keys"
    val RadixTreeNodes: String  = "radix-tree-nodes"
    val Groups: String          = "groups"
    val LastGeneratedId: String = "last-generated-id"
    val FirstEntry: String      = "first-entry"
    val LastEntry: String       = "last-entry"

    val MaxDeletedEntryId: String    = "max-deleted-entry-id"
    val EntriesAdded: String         = "entries-added"
    val RecordedFirstEntryId: String = "recorded-first-entry-id"

    val Entries: String    = "entries"
    val PelCount: String   = "pel-count"
    val SeenTime: String   = "seen-time"
    val ActiveTime: String = "active-time"
  }
}
