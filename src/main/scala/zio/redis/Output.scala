package zio.redis

import zio.Chunk
import zio.duration.Duration

sealed trait Output[+A] {
  def decode(text: Chunk[Byte]): Either[RedisError, A] = ???
}

object Output {
  type Bytes = Chunk[Byte]

  case object BoolOutput     extends Output[Boolean]
  case object ByteOutput     extends Output[Bytes]
  case object ChunkOutput    extends Output[Chunk[Bytes]]
  case object DoubleOutput   extends Output[Double]
  case object DurationOutput extends Output[Duration]
  case object LongOutput     extends Output[Long]

  final case class OptionalOutput[+A](output: Output[A]) extends Output[Option[A]]

  case object ScanOutput   extends Output[(Long, Chunk[Bytes])]
  case object StringOutput extends Output[String]
  case object UnitOutput   extends Output[Unit]
}
