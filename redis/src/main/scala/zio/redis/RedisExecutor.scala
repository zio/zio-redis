package zio.redis

import zio.{Chunk, IO, ULayer, ZLayer}

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.layer.fresh >>> SingleNodeExecutor.layer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.default.fresh >>> SingleNodeExecutor.layer

  lazy val test: ULayer[RedisExecutor] =
    TestExecutor.layer
}
