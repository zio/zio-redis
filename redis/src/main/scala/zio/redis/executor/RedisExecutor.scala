package zio.redis.executor

import zio.clock.Clock
import zio.logging.Logging
import zio.redis.executor.node.RedisNodeExecutorLive
import zio.redis.executor.test.RedisTestExecutorLive
import zio.redis.{RedisError, RedisUri, RespValue}
import zio.{Chunk, Has, IO, URLayer, ZLayer}

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {
  lazy val live: ZLayer[Logging with Has[RedisUri], RedisError.IOError, Has[RedisExecutor]] =
    ZLayer.identity[Logging] ++ RedisConnectionLive.layer >>> RedisNodeExecutorLive.layer

  lazy val local: ZLayer[Logging, RedisError.IOError, Has[RedisExecutor]] =
    ZLayer.identity[Logging] ++ RedisConnectionLive.default >>> RedisNodeExecutorLive.layer

  lazy val test: URLayer[zio.random.Random with Clock, Has[RedisExecutor]] =
    RedisTestExecutorLive.layer
}
