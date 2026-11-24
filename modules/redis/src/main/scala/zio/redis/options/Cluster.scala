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

import zio.Chunk
import zio.redis.RedisUri

/**
 * Types for Redis Cluster: hash slots, nodes and slot partitions.
 */
object Cluster {
  private[redis] final val SlotsAmount = 16384

  /**
   * A hash slot number. Redis Cluster maps every key to one of its hash slots. The constructor does not
   * check the range.
   */
  final case class Slot(number: Long) extends AnyVal

  /**
   * Companion object of `Slot`.
   */
  object Slot {

    /**
     * The slot with number 1.
     */
    final val Default: Slot = Slot(1)
  }

  /**
   * A cluster node: its node ID and the `RedisUri` that clients connect to.
   */
  final case class Node(id: String, address: RedisUri)

  /**
   * An inclusive range of hash slots, from `start` to `end`.
   */
  final case class SlotRange(start: Long, end: Long) {

    /**
     * Returns `true` when `slot.number` lies between `start` and `end`, both included. A range whose `start` is
     * greater than `end` contains no slot.
     */
    def contains(slot: Slot): Boolean = start <= slot.number && slot.number <= end
  }

  /**
   * The slots served by one master node and its replicas, as returned by `CLUSTER SLOTS`.
   */
  final case class Partition(slotRange: SlotRange, master: Node, slaves: Chunk[Node]) {

    /**
     * The master, followed by the replicas in the order of `slaves`.
     */
    lazy val nodes: Chunk[Node] = master +: slaves

    /**
     * The `RedisUri` of each node in `nodes`, in the same order.
     */
    lazy val addresses: Chunk[RedisUri] = nodes.map(_.address)
  }

  /**
   * The sub-command of `CLUSTER SETSLOT`, which changes the state of one hash slot.
   */
  sealed trait SetSlotSubCommand extends Product { self =>
    private[redis] final def asString: String =
      self match {
        case SetSlotSubCommand.Migrating => "MIGRATING"
        case SetSlotSubCommand.Importing => "IMPORTING"
        case SetSlotSubCommand.Stable    => "STABLE"
        case SetSlotSubCommand.Node      => "NODE"
      }
  }

  /**
   * The sub-commands of `CLUSTER SETSLOT`.
   */
  object SetSlotSubCommand {

    /**
     * Marks a slot as migrating away from this node. Sent as `MIGRATING`.
     */
    case object Migrating extends SetSlotSubCommand

    /**
     * Marks a slot as being imported into this node. Sent as `IMPORTING`.
     */
    case object Importing extends SetSlotSubCommand

    /**
     * Clears the migrating and importing state of a slot. Sent as `STABLE`.
     */
    case object Stable extends SetSlotSubCommand

    /**
     * Assigns a slot to the node with the given ID. Sent as `NODE`.
     */
    case object Node extends SetSlotSubCommand
  }
}
