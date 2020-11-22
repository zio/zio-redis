package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.Chunk

trait Cluster {
  import Cluster._

  final def clusterInfo(): ZIO[RedisExecutor, RedisError, ClusterInfo] =
    InfoC.run(())

  final def clusterNodes(): ZIO[RedisExecutor, RedisError, Chunk[ClusterNode]] =
    NodesC.run(Nodes)
}

private object Cluster {
  final val InfoC = RedisCommand("CLUSTER INFO", NoInput, ClusterInfoOutput, Key.NoKey)

  final val NodesC = RedisCommand("CLUSTER", NodesInput, ClusterNodesOutput, Key.NoKey)
}
