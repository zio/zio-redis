package example

import zio.json.{DeriveJsonCodec, JsonCodec}

final case class Contributor(login: Login, contributions: Contributions)

object Contributor {
  implicit val codec: JsonCodec[Contributor] =
    DeriveJsonCodec.gen[Contributor]

}
