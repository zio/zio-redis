package zio.redis.executor.cluster

import zio.Chunk

final case class RedisUri(host: String, port: Int) {
  override def toString: String = s"$host:$port"
}

object RedisUri {
  def apply(hostAndPort: String): RedisUri = {
    val splitting = hostAndPort.split(':')
    val host      = splitting(0)
    val port      = splitting(1).toInt
    RedisUri(host, port)
  }
}

final case class RedisClusterConfig(addresses: Chunk[RedisUri])
