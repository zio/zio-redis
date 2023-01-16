package zio.redis

import zio.schema.codec.BinaryCodec

trait RedisEnvironment {
  def codec: BinaryCodec
  def executor: RedisExecutor
}
