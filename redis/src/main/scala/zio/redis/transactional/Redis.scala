package zio.redis.transactional

import zio.redis.RedisExecutor
import zio.schema.codec.BinaryCodec
import zio.{URLayer, ZLayer}

trait Redis extends api.Sets

private[transactional] final case class RedisLive(codec: BinaryCodec, executor: RedisExecutor) extends Redis

object RedisLive {
  lazy val layer: URLayer[RedisExecutor with BinaryCodec, Redis] =
    ZLayer.fromFunction(RedisLive.apply _)
}
