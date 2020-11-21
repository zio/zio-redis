package zio.redis

import zio.test._
import zio.test.Assertion._

trait ConnectionSpec extends BaseSpec {

  val connectionSuite = suite("connection")(
    testM("PING with no input") {
      ping().map(assert(_)(equalTo("PONG")))
    },
    testM("PING with input") {
      ping(Some("Hello")).map(s => assert(s)(equalTo("Hello")))
    }
  )

}
