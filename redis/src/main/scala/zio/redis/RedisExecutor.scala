package zio.redis

import zio.{Chunk, IO, Ref, ULayer, ZLayer}
import zio.stream._

trait RedisExecutor {
  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]

  def executePubSub(command: Chunk[RespValue.BulkString]): Stream[RedisError, RespValue]
}

object RedisExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.layer.fresh >>> SingleNodeExecutor.layer

  lazy val local: ZLayer[Any, RedisError.IOError, RedisExecutor] =
    RedisConnectionLive.default.fresh >>> SingleNodeExecutor.layer

  lazy val test: ULayer[RedisExecutor] =
    TestExecutor.layer
}
