package zio.redis.internal

import zio._
import zio.redis._
import zio.redis.options.Cluster.Partition
import zio.redis.options.Cluster.Slot

private[redis] final case class ClusterConnection(
  partitions: Chunk[Partition],
  executors: Map[RedisUri, ExecutorScope],
  slots: Map[Slot, RedisUri]
) {
  def executor(slot: Slot): Option[RedisExecutor] = executors.get(slots(slot)).map(_.executor)

  def addExecutor(uri: RedisUri, es: ExecutorScope): ClusterConnection =
    copy(executors = executors + (uri -> es))
}
