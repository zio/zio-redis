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

package zio.redis.internal

import zio._
import zio.redis._
import zio.redis.options.Cluster.{Partition, Slot}

private[redis] final case class ClusterConnection(
  partitions: Chunk[Partition],
  executors: Map[RedisUri, ExecutorScope],
  slots: Map[Slot, RedisUri]
) {
  def executor(slot: Slot): Option[RedisExecutor] = executors.get(slots(slot)).map(_.executor)

  def addExecutor(uri: RedisUri, es: ExecutorScope): ClusterConnection =
    copy(executors = executors + (uri -> es))
}
