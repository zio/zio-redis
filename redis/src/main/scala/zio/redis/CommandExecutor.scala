package zio.redis

import zio.schema.codec.BinaryCodec

trait CommandExecutor {
  implicit def codec: BinaryCodec

  implicit def executor: RedisExecutor
}
