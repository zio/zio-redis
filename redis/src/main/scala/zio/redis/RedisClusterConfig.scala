package zio.redis

import zio.Chunk

final case class RedisClusterConfig(addresses: Chunk[RedisUri])
