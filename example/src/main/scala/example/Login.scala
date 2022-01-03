package example

import zio.json.JsonCodec
import zio.prelude.Newtype

object Login extends Newtype[String] {
  implicit val codec: JsonCodec[Login] =
    JsonCodec.string.xmap(Login(_), Login.unwrap)
}
