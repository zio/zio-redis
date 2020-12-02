package example

import io.circe.generic.semiauto._
import zio.Chunk
import io.circe.Encoder

final case class Contributors(contributors: Chunk[Contributor]) extends AnyVal

object Contributors {
  implicit val encoder: Encoder[Contributors] = deriveEncoder[Contributors]
}
