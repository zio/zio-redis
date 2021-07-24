package example

import zio.Chunk
import zio.json.JsonCodec

final case class Contributors(contributors: Chunk[Contributor]) extends AnyVal

object Contributors {
  implicit val codec: JsonCodec[Contributors] =
    JsonCodec.chunk[Contributor].xmap(Contributors(_), _.contributors)

}
