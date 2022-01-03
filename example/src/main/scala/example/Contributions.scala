package example

import zio.json._
import zio.prelude.Newtype

object Contributions extends Newtype[Int] {
  implicit val codec: JsonCodec[Contributions] = JsonCodec.int.xmap(Contributions(_), Contributions.unwrap)
}
