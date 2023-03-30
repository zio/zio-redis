package zio.redis

import zio.{URLayer, ZIO, ZLayer}
import zio.schema.codec.BinaryCodec

trait RedisSubscription extends api.Subscribe

object RedisSubscription {
  lazy val layer: URLayer[SubscriptionExecutor with BinaryCodec, RedisSubscription] =
    ZLayer {
      for {
        codec    <- ZIO.service[BinaryCodec]
        executor <- ZIO.service[SubscriptionExecutor]
      } yield Live(codec, executor)
    }

  private final case class Live(codec: BinaryCodec, executor: SubscriptionExecutor) extends RedisSubscription
}
