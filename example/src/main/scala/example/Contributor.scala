package example

import io.circe.{ Decoder, Encoder }

final case class Contributor(login: Login, contributions: Contributions)

object Contributor {
  implicit val decoder: Decoder[Contributor] =
    Decoder[(String, Int)].map { case (login, contributions) =>
      Contributor(Login(login), Contributions(contributions))
    }

  implicit val encoder: Encoder[Contributor] =
    Encoder[(String, Int)].contramap(c => (Login.unwrap(c.login), Contributions.unwrap(c.contributions)))
}
