package example

import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.parser.decode

import zio.prelude._
import zio.{Chunk, IO, NonEmptyChunk, ZIO}

final case class Contributors(contributors: Chunk[Contributor]) extends AnyVal

object Contributors {
  implicit val encoder: Encoder[Contributors] = deriveEncoder[Contributors]

  private[example] def make(raw: Chunk[String]): IO[ApiError, Contributors] =
    ZIO.fromEither {
      NonEmptyChunk.fromChunk(raw) match {
        case Some(raw) => raw.foreach1(decode[Contributor]).map(Contributors(_)).left.map(_ => ApiError.CorruptedData)
        case None      => Left(ApiError.CacheMiss)
      }
    }
}
