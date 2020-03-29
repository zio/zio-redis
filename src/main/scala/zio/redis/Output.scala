package zio.redis

import zio.{ Chunk, IO }
import zio.stream.Stream
import zio.duration.Duration

sealed trait Output[+A]

object Output {
  case object BoolOutput                           extends Output[IO[Error, Boolean]]
  case object ByteOutput                           extends Output[IO[Error, Chunk[Byte]]]
  case object DoubleOutput                         extends Output[IO[Error, Double]]
  case object DurationOutput                       extends Output[IO[Error, Duration]]
  case object LongOutput                           extends Output[IO[Error, Long]]
  case object ScanOutput                           extends Output[IO[Error, (Long, Stream[Error, Chunk[Byte]])]]
  case object StreamOutput                         extends Output[Stream[Error, Chunk[Byte]]]
  case object StringOutput                         extends Output[IO[Error, String]]
  case object UnitOutput                           extends Output[IO[Error, Unit]]
  case class OptionalOutput[+A](output: Output[A]) extends Output[IO[Error, Option[A]]]
}
