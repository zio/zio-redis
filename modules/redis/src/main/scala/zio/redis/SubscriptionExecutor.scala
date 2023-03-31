package zio.redis

import zio.redis.internal.{RedisConnection, RespCommand, RespValue}
import zio.stream._
import zio.{IO, Layer, ZIO, ZLayer}

trait SubscriptionExecutor {
  private[redis] def execute(command: RespCommand): IO[RedisError, Stream[RedisError, RespValue]]
}

object SubscriptionExecutor {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, SubscriptionExecutor] =
    RedisConnection.layer.fresh >>> pubSublayer

  lazy val local: Layer[RedisError.IOError, SubscriptionExecutor] =
    RedisConnection.local.fresh >>> pubSublayer

  private lazy val pubSublayer: ZLayer[RedisConnection, RedisError.IOError, SubscriptionExecutor] =
    ZLayer.scoped(
      for {
        conn   <- ZIO.service[RedisConnection]
        pubSub <- SingleNodeSubscriptionExecutor.create(conn)
      } yield pubSub
    )
}
