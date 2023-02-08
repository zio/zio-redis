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

import zio.redis.{RedisExecutor, RedisUri}
import zio.{Chunk, Scope}

object Cluster {

  private[redis] final val SlotsAmount = 16384

  final case class ExecutorScope(executor: RedisExecutor, scope: Scope.Closeable)

  final case class ClusterConnection(
    partitions: Chunk[Partition],
    executors: Map[RedisUri, ExecutorScope],
    slots: Map[Slot, RedisUri]
  ) {
    def executor(slot: Slot): Option[RedisExecutor] = executors.get(slots(slot)).map(_.executor)

    def addExecutor(uri: RedisUri, es: ExecutorScope): ClusterConnection =
      copy(executors = executors + (uri -> es))
  }

  final case class Slot(number: Long) extends AnyVal

  final case class Node(id: String, address: RedisUri)

  final case class SlotRange(start: Long, end: Long) {
    def contains(slot: Slot): Boolean = start <= slot.number && slot.number <= end
  }

  final case class Partition(slotRange: SlotRange, master: Node, slaves: Chunk[Node]) {
    lazy val nodes: Chunk[Node]         = master +: slaves
    lazy val addresses: Chunk[RedisUri] = nodes.map(_.address)
  }

  sealed trait SetSlotSubCommand extends Product { self =>
    private[redis] final def stringify: String =
      self match {
        case SetSlotSubCommand.Migrating => "MIGRATING"
        case SetSlotSubCommand.Importing => "IMPORTING"
        case SetSlotSubCommand.Stable    => "STABLE"
        case SetSlotSubCommand.Node      => "NODE"
      }
  }

  object SetSlotSubCommand {
    case object Migrating extends SetSlotSubCommand
    case object Importing extends SetSlotSubCommand
    case object Stable    extends SetSlotSubCommand
    case object Node      extends SetSlotSubCommand
  }
}
