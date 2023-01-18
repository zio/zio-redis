package zio.redis

import zio.schema.codec.BinaryCodec

private[redis] trait RedisEnvironment {
  def codec: BinaryCodec
  def executor: RedisExecutor
}
