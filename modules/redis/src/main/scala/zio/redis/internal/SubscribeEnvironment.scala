package zio.redis

import zio.schema.codec.BinaryCodec

trait SubscribeEnvironment {
  protected def codec: BinaryCodec
  protected def executor: SubscriptionExecutor
}
