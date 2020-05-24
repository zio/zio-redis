package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  private[redis] def decode(blob: Chunk[Byte]): Either[RedisError, A]
}

object Output {
  case object BoolOutput extends Output[Boolean] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Boolean] = ???
  }

  case object ByteOutput extends Output[Chunk[Byte]] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Chunk[Byte]] = ???
  }

  case object ChunkOutput extends Output[Chunk[Chunk[Byte]]] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Chunk[Chunk[Byte]]] = ???
  }

  case object DoubleOutput extends Output[Double] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Double] = ???
  }

  case object DurationOutput extends Output[Duration] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Duration] = ???
  }

  case object LongOutput extends Output[Long] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Long] = ???
  }

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Option[A]] = ???
  }

  case object ScanOutput extends Output[(Long, Chunk[Chunk[Byte]])] {
    def decode(blob: Chunk[Byte]): Either[RedisError, (Long, Chunk[Chunk[Byte]])] = ???
  }

  case object StringOutput extends Output[String] {
    def decode(blob: Chunk[Byte]): Either[RedisError, String] = ???
  }

  case object UnitOutput extends Output[Unit] {
    def decode(blob: Chunk[Byte]): Either[RedisError, Unit] = ???
  }
}
