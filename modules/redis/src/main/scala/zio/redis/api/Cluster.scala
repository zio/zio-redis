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

import zio.redis.Input._
import zio.redis.Output.{ChunkOutput, ClusterPartitionOutput, UnitOutput}
import zio.redis._
import zio.redis.api.Cluster.{AskingCommand, ClusterSetSlots, ClusterSlots}
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.redis.options.Cluster.SetSlotSubCommand._
import zio.redis.options.Cluster.{Partition, Slot}
import zio.{Chunk, IO}

trait Cluster extends RedisEnvironment {

  /**
   * When a cluster client receives an -ASK redirect, the ASKING command is sent to the target node followed by the
   * command which was redirected.
   *
   * @return
   *   the Unit value.
   */
  final def asking: IO[RedisError, Unit] =
    AskingCommand(executor).run(())

  /**
   * Returns details about which cluster slots map to which Redis instances.
   *
   * @return
   *   details about which cluster
   */
  final def slots: IO[RedisError, Chunk[Partition]] = {
    val command = RedisCommand(ClusterSlots, NoInput, ChunkOutput(ClusterPartitionOutput), executor)
    command.run(())
  }

  /**
   * Clear any importing / migrating state from hash slot.
   *
   * @param slot
   *   hash slot that will be cleared
   * @return
   *   the Unit value.
   */
  final def setSlotStable(slot: Slot): IO[RedisError, Unit] = {
    val command =
      RedisCommand(ClusterSetSlots, Tuple2(LongInput, ArbitraryValueInput[String]()), UnitOutput, executor)
    command.run((slot.number, Stable.asString))
  }

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
  final def setSlotMigrating(slot: Slot, nodeId: String): IO[RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
      UnitOutput,
      executor
    )
    command.run((slot.number, Migrating.asString, nodeId))
  }

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
  final def setSlotImporting(slot: Slot, nodeId: String): IO[RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
      UnitOutput,
      executor
    )
    command.run((slot.number, Importing.asString, nodeId))
  }

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
  final def setSlotNode(slot: Slot, nodeId: String): IO[RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
      UnitOutput,
      executor
    )
    command.run((slot.number, Node.asString, nodeId))
  }
}

private[redis] object Cluster {
  final val Asking          = "ASKING"
  final val ClusterSlots    = "CLUSTER SLOTS"
  final val ClusterSetSlots = "CLUSTER SETSLOT"

  final val AskingCommand: (RedisExecutor) => RedisCommand[Unit, Unit] =
    (executor: RedisExecutor) => RedisCommand(Asking, NoInput, UnitOutput, executor)
}
