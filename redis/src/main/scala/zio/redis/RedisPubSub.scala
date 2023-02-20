package zio.redis

import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, IO, ZIO, ZLayer}

trait RedisPubSub {
  def execute(command: PubSubCommand): IO[RedisError, Chunk[Stream[RedisError, RespValue]]]
}

object RedisPubSub {
  lazy val layer: ZLayer[RedisConfig with BinaryCodec, RedisError.IOError, RedisPubSub] =
    RedisConnectionLive.layer.fresh >>> pubSublayer

  lazy val local: ZLayer[BinaryCodec, RedisError.IOError, RedisPubSub] =
    RedisConnectionLive.default.fresh >>> pubSublayer

  private lazy val pubSublayer: ZLayer[RedisConnection with BinaryCodec, RedisError.IOError, RedisPubSub] =
    ZLayer.scoped(
      for {
        conn   <- ZIO.service[RedisConnection]
        codec  <- ZIO.service[BinaryCodec]
        pubSub <- SingleNodeRedisPubSub.create(conn, codec)
      } yield pubSub
    )
}
