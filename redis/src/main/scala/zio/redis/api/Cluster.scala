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
import zio.redis.options.Cluster.SetSlotSubCommand._
import zio.redis.options.Cluster.{Partition, Slot}
import zio.{Chunk, ZIO}

trait Cluster {

  def asking: ZIO[Redis, RedisError, Unit] =
    AskingCommand.run(())

  def slots: ZIO[Redis, RedisError, Chunk[Partition]] = {
    val command = RedisCommand(ClusterSlots, NoInput, ChunkOutput(ClusterPartitionOutput))
    command.run(())
  }

  def setSlotStable(slot: Slot): ZIO[Redis, RedisError, Unit] = {
    val command = RedisCommand(ClusterSetSlots, Tuple2(LongInput, ArbitraryInput[String]()), UnitOutput)
    command.run((slot.number, Stable.stringify))
  }

  def setSlotMigrating(slot: Slot, destinationNodeId: String): ZIO[Redis, RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryInput[String](), ArbitraryInput[String]()),
      UnitOutput
    )
    command.run((slot.number, Migrating.stringify, destinationNodeId))
  }

  def setSlotImporting(slot: Slot, sourceNodeId: String): ZIO[Redis, RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryInput[String](), ArbitraryInput[String]()),
      UnitOutput
    )
    command.run((slot.number, Importing.stringify, sourceNodeId))
  }

  def setSlotNode(slot: Slot, nodeId: String): ZIO[Redis, RedisError, Unit] = {
    val command = RedisCommand(
      ClusterSetSlots,
      Tuple3(LongInput, ArbitraryInput[String](), ArbitraryInput[String]()),
      UnitOutput
    )
    command.run((slot.number, Node.stringify, nodeId))
  }
}

private[redis] object Cluster {
  final val Asking          = "ASKING"
  final val ClusterSlots    = "CLUSTER SLOTS"
  final val ClusterSetSlots = "CLUSTER SETSLOT"

  final val AskingCommand: RedisCommand[Unit, Unit] = RedisCommand(Asking, NoInput, UnitOutput)
}
