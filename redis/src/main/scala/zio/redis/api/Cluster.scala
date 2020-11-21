package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Cluster {
  import Cluster._

  final def info(): ZIO[RedisExecutor, RedisError, ClusterInfo] =
    Info.run(())
}

private object Cluster {
  final val Info = RedisCommand("CLUSTER INFO", NoInput, ClusterInfoOutput)
}
