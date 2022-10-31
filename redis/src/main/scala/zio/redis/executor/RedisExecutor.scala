package zio.redis.executor

import zio.redis.executor.node.RedisNodeExecutorLive
import zio.redis.executor.test.RedisTestExecutorLive
import zio.redis.{RedisConfig, RedisError, RespValue}
import zio.{Chunk, IO, ULayer, ZLayer}

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.layer >>> RedisNodeExecutorLive.layer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.default >>> RedisNodeExecutorLive.layer

  lazy val test: ULayer[RedisExecutor] =
    RedisTestExecutorLive.layer
}
