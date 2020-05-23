package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  private[redis] def decode(blob: Chunk[Byte]): Either[RedisError, A] = ???
}

object Output {
  case object BoolOutput     extends Output[Boolean]
  case object ByteOutput     extends Output[Chunk[Byte]]
  case object ChunkOutput    extends Output[Chunk[Chunk[Byte]]]
  case object DoubleOutput   extends Output[Double]
  case object DurationOutput extends Output[Duration]
  case object LongOutput     extends Output[Long]

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]]

  case object ScanOutput   extends Output[(Long, Chunk[Chunk[Byte]])]
  case object StringOutput extends Output[String]
  case object UnitOutput   extends Output[Unit]
}
