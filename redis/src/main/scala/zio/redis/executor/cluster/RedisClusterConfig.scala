package zio.redis.executor.cluster

import zio.Chunk
import zio.redis.RedisUri

final case class RedisClusterConfig(addresses: Chunk[RedisUri])
