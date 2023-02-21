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

package zio.redis.api

import zio.redis._
import zio.redis.options.Cluster.SetSlotSubCommand._
import zio.redis.options.Cluster.{Partition, Slot}
import zio.{Chunk, IO}

trait Cluster extends commands.Cluster {

  /**
   * When a cluster client receives an -ASK redirect, the ASKING command is sent to the target node followed by the
   * command which was redirected.
   *
   * @return
   *   the Unit value.
   */
  final def asking: IO[RedisError, Unit] = _asking.run(())

  /**
   * Returns details about which cluster slots map to which Redis instances.
   *
   * @return
   *   details about which cluster
   */
  final def slots: IO[RedisError, Chunk[Partition]] = _slots.run(())

  /**
   * Clear any importing / migrating state from hash slot.
   *
   * @param slot
   *   hash slot that will be cleared
   * @return
   *   the Unit value.
   */

  final def setSlotStable(slot: Slot): IO[RedisError, Unit] = _setSlotStable.run((slot.number, Stable.stringify))

  /**
   * Set a hash slot in migrating state. Command should be executed on the node from which hash slot will be imported
   * (source node)
   *
   * @param slot
   *   hash slot that will be set in migrating state
   * @param nodeId
   *   the destination node Id where slot will be migrated
   * @return
   *   the Unit value.
   */
  final def setSlotMigrating(slot: Slot, nodeId: String): IO[RedisError, Unit] =
    _setSlotMigrating.run((slot.number, Migrating.stringify, nodeId))

  /**
   * Set a hash slot in importing state. Command should be executed on the node where hash slot will be migrated
   * (destination node)
   *
   * @param slot
   *   hash slot that will be set in importing state
   * @param nodeId
   *   the source node Id from which hash slot will be imported
   * @return
   *   the Unit value.
   */

  final def setSlotImporting(slot: Slot, nodeId: String): IO[RedisError, Unit] =
    _setSlotImporting.run((slot.number, Importing.stringify, nodeId))

  /**
   * Bind the hash slot to a different node. It associates the hash slot with the specified node, however the command
   * works only in specific situations and has different side effects depending on the slot state.
   *
   * @param slot
   *   hash slot that will be bind
   * @param nodeId
   *   the destination node Id where slot have been migrated
   * @return
   *   the Unit value.
   */
  final def setSlotNode(slot: Slot, nodeId: String): IO[RedisError, Unit] =
    _setSlotNode.run((slot.number, Node.stringify, nodeId))
}
