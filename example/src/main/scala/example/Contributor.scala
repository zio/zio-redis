package example

import io.circe.generic.semiauto._
import io.circe.{ Decoder, _ }

final case class Contributor(login: String, contributions: Int)

object Contributor {
  implicit val decoder: Decoder[Contributor] = deriveDecoder[Contributor]
  implicit val encoder: Encoder[Contributor] = deriveEncoder[Contributor]
}
