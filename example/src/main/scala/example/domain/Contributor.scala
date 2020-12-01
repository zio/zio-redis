package example.domain

import io.circe.Decoder
import io.circe._
import io.circe.generic.semiauto._

final case class Contributor(login: String, contributions: Int)

object Contributor {
  implicit val decoder: Decoder[Contributor] = deriveDecoder[Contributor]
  implicit val encoder: Encoder[Contributor] = deriveEncoder[Contributor]
}
