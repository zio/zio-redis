package example

import io.circe.{ Encoder, Decoder }
import zio.prelude.Newtype

object Login extends Newtype[String] {
  implicit val decoder: Decoder[Login] = Decoder[String].map(Login(_))

  implicit val encoder: Encoder[Login] = Encoder[String].contramap(Login.unwrap)
}
