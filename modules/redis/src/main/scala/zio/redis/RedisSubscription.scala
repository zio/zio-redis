package zio.redis

import zio._

trait RedisSubscription extends api.Subscription

object RedisSubscription {
  lazy val local: ZLayer[CodecSupplier, RedisError.IOError, RedisSubscription] =
    SubscriptionExecutor.local >>> makeLayer

  lazy val singleNode: ZLayer[CodecSupplier & RedisConfig, RedisError.IOError, RedisSubscription] =
    SubscriptionExecutor.layer >>> makeLayer

  private def makeLayer: URLayer[CodecSupplier & SubscriptionExecutor, RedisSubscription] =
    ZLayer.fromFunction(Live.apply _)

  private final case class Live(codecSupplier: CodecSupplier, executor: SubscriptionExecutor) extends RedisSubscription
}
