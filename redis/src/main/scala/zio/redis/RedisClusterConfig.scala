package zio.redis

import zio.{Chunk, Duration, durationInt}

final case class RedisClusterConfig(addresses: Chunk[RedisUri], retry: RetryClusterConfig = RetryClusterConfig.Default)

final case class RetryClusterConfig(base: Duration, factor: Double, maxRecurs: Int)

object RetryClusterConfig {
  lazy val Default: RetryClusterConfig = RetryClusterConfig(100.millis, 1.5, 5)
}
