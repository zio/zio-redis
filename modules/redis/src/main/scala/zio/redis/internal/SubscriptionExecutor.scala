package zio.redis

import zio.stream._
import zio.{IO, Layer, ZIO, ZLayer}

trait SubscriptionExecutor {
  def execute(command: RespCommand): IO[RedisError, Stream[RedisError, RespValue]]
}

object SubscriptionExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, SubscriptionExecutor] =
    RedisConnectionLive.layer.fresh >>> pubSublayer

  lazy val local: Layer[RedisError.IOError, SubscriptionExecutor] =
    RedisConnectionLive.default.fresh >>> pubSublayer

  private lazy val pubSublayer: ZLayer[RedisConnection, RedisError.IOError, SubscriptionExecutor] =
    ZLayer.scoped(
      for {
        conn   <- ZIO.service[RedisConnection]
        pubSub <- SingleNodeSubscriptionExecutor.create(conn)
      } yield pubSub
    )
}
