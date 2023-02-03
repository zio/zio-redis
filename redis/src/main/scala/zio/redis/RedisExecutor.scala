package zio.redis

import zio.{Chunk, IO, ZLayer}

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
}

object RedisExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.layer >>> SingleNodeExecutor.layer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.default >>> SingleNodeExecutor.layer
}
