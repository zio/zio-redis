package example

import io.circe.{ Decoder, Encoder }

import zio.prelude.Newtype

object Contributions extends Newtype[Int] {
  implicit val decoder: Decoder[Contributions] = Decoder[Int].map(Contributions(_))

  implicit val encoder: Encoder[Contributions] = Encoder[Int].contramap(Contributions.unwrap)
}
