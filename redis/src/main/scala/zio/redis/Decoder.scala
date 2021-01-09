package zio.redis

import zio.IO

trait Decoder[A] {
  def decode(respValue: RespValue): IO[RedisError, A]
}
