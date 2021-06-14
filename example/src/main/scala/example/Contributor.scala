package example

import io.circe.{ Decoder, Encoder }

final case class Contributor(login: Login, contributions: Contributions)

object Contributor {
  implicit val decoder: Decoder[Contributor] =
    Decoder.forProduct2("login", "contributions")(Contributor.apply)

  implicit val encoder: Encoder[Contributor] =
    Encoder.forProduct2("login", "contributions")(c => (c.login, c.contributions))
}
