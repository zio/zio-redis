package zio.redis

import zio.Chunk

import java.nio.charset.StandardCharsets

trait RedisEncoder[A] {
  def encode(a: A): Chunk[Byte]
}

object RedisEncoder {
  implicit val bytesEncoder: RedisEncoder[Chunk[Byte]] = identity

  implicit val booleanEncoder: RedisEncoder[Boolean] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }

  implicit val stringEncoder: RedisEncoder[String] = { value =>
    Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8))
  }

  implicit val intEncoder: RedisEncoder[Int] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }

  implicit val longEncoder: RedisEncoder[Long] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }
}
