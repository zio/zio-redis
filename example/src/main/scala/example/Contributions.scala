package example

import io.circe.{ Encoder, Decoder }
import zio.prelude.Newtype

object Contributions extends Newtype[Int] {
  implicit val decoder: Decoder[Contributions] = Decoder[Int].map(Contributions(_))

  implicit val encoder: Encoder[Contributions] = Encoder[Int].contramap(Contributions.unwrap)
}
