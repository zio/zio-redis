package zio.redis

import zio.{URLayer, ZIO, ZLayer}

trait RedisSubscription extends api.Subscription

object RedisSubscription {
  lazy val layer: URLayer[SubscriptionExecutor with CodecSupplier, RedisSubscription] =
    ZLayer {
      for {
        codecSupplier <- ZIO.service[CodecSupplier]
        executor      <- ZIO.service[SubscriptionExecutor]
      } yield Live(codecSupplier, executor)
    }

  private final case class Live(codecSupplier: CodecSupplier, executor: SubscriptionExecutor) extends RedisSubscription
}
