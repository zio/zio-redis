package zio.redis

import zio.IO

trait RedisDecoder[A] {
  def decode(respValue: RespValue): IO[RedisError, A]
}
