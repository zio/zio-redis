package example

import io.circe.Codec
import io.circe.generic.semiauto._

import zio.Chunk

final case class Contributors(contributors: Chunk[Contributor]) extends AnyVal

object Contributors {
  implicit val codec: Codec[Contributors] = deriveCodec[Contributors]
}
