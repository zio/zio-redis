package example

import io.circe.Codec
import io.circe.generic.semiauto._

final case class Contributor(login: Login, contributions: Contributions)

object Contributor {
  implicit val codec: Codec[Contributor] = deriveCodec[Contributor]
}
