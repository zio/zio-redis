package zio.redis

import zio.duration.Duration

case class PoolConfig(
  host: String,
  port: Int,
  baseMaxSize: Int,
  baseMaxIdleTimeout: Duration,
  streamsMaxSize: Int,
  streamsMaxIdleTimeout: Duration,
  transactionsMaxSize: Int,
  transactionsMaxIdleTimeout: Duration
)
