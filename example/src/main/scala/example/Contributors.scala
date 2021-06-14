package example

import io.circe.{ Decoder, Encoder }

import zio.Chunk

final case class Contributors(contributors: Chunk[Contributor]) extends AnyVal

object Contributors {
  implicit val decoder: Decoder[Contributors] =
    Decoder.forProduct1("contributors")(Contributors(_))

  implicit val encoder: Encoder[Contributors] =
    Encoder.forProduct1("contributors")(_.contributors)
}
