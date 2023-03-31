package zio.redis.internal

import zio.redis.{CodecSupplier, SubscriptionExecutor}
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

private[redis] trait SubscribeEnvironment {
  protected def codecSupplier: CodecSupplier
  protected def executor: SubscriptionExecutor
  protected final implicit def codec[A: Schema]: BinaryCodec[A] = codecSupplier.get
}
