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

package zio.redis.commands

import zio.Chunk
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.schema.codec.BinaryCodec

private[redis] trait Cluster extends RedisEnvironment {
  import Cluster._

  final val _asking: RedisCommand[Unit, Unit] = askingCommand(codec, executor)

  final val _slots: RedisCommand[Unit, Chunk[zio.redis.options.Cluster.Partition]] =
    RedisCommand(ClusterSlots, NoInput, ChunkOutput(ClusterPartitionOutput), codec, executor)

  final val _setSlotStable: RedisCommand[(Long, String), Unit] =
    RedisCommand(ClusterSetSlots, Tuple2(LongInput, ArbitraryValueInput[String]()), UnitOutput, codec, executor)

  final val _setSlotMigrating: RedisCommand[(Long, String, String), Unit] = RedisCommand(
    ClusterSetSlots,
    Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    UnitOutput,
    codec,
    executor
  )

  final val _setSlotImporting: RedisCommand[(Long, String, String), Unit] = RedisCommand(
    ClusterSetSlots,
    Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    UnitOutput,
    codec,
    executor
  )

  final val _setSlotNode: RedisCommand[(Long, String, String), Unit] = RedisCommand(
    ClusterSetSlots,
    Tuple3(LongInput, ArbitraryValueInput[String](), ArbitraryValueInput[String]()),
    UnitOutput,
    codec,
    executor
  )
}

private[redis] object Cluster {
  private final val Asking          = "ASKING"
  private final val ClusterSlots    = "CLUSTER SLOTS"
  private final val ClusterSetSlots = "CLUSTER SETSLOT"

  final val askingCommand: (BinaryCodec, RedisExecutor) => RedisCommand[Unit, Unit] =
    RedisCommand(Asking, NoInput, UnitOutput, _, _)
}
