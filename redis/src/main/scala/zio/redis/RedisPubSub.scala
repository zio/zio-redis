package zio.redis

import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{ULayer, ZIO, ZLayer}

trait RedisPubSub {
  def execute(command: RedisPubSubCommand): ZStream[BinaryCodec, RedisError, PushProtocol]
}

object RedisPubSub {
  lazy val layer: ZLayer[RedisConfig with BinaryCodec, RedisError.IOError, RedisPubSub] =
    RedisConnectionLive.layer.fresh >>> pubSublayer

  lazy val local: ZLayer[BinaryCodec, RedisError.IOError, RedisPubSub] =
    RedisConnectionLive.default.fresh >>> pubSublayer

  lazy val test: ULayer[RedisPubSub] =
    TestExecutor.layer

  private lazy val pubSublayer: ZLayer[RedisConnection with BinaryCodec, RedisError.IOError, RedisPubSub] =
    ZLayer.scoped(
      ZIO.service[RedisConnection].flatMap(SingleNodeRedisPubSub.create(_))
    )
}
