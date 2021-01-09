package zio.redis

import java.nio.charset.StandardCharsets

import zio.Chunk

trait Encoder[A] {
  def encode(a: A): Chunk[Byte]
}

object Encoder {
  implicit val bytesEncoder: Encoder[Chunk[Byte]] = identity

  implicit val booleanEncoder: Encoder[Boolean] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }

  implicit val stringEncoder: Encoder[String] = { value =>
    Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8))
  }

  implicit val intEncoder: Encoder[Int] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }

  implicit val longEncoder: Encoder[Long] = { value =>
    Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
  }
}
