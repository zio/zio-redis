package zio.redis

import zio.prelude.Assertion.greaterThanOrEqualTo
import zio.prelude.Newtype

package object options {
  object NonNegativeLong extends Newtype[Long] {
    override def assertion = assert(greaterThanOrEqualTo(0L)) // scalafix:ok
  }

  type NonNegativeLong = NonNegativeLong.Type
}
