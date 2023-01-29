package zio.redis

import zio._
import zio.schema.codec.BinaryCodec

final case class RedisEnvironment(codec: BinaryCodec, executor: RedisExecutor)

private[redis] object RedisEnvironment {
  lazy val layer = ZLayer {
    for {
      codec    <- ZIO.service[BinaryCodec]
      executor <- ZIO.service[RedisExecutor]
    } yield new RedisEnvironment(codec, executor)
  }
}
