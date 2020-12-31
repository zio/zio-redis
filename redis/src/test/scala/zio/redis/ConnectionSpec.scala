package zio.redis

import zio.test.Assertion._
import zio.test._

trait ConnectionSpec extends BaseSpec {

  val connectionSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("connection")(
      testM("PING with no input") {
        ping(None).map(assert(_)(equalTo("PONG")))
      },
      testM("PING with input") {
        ping(Some("Hello")).map(assert(_)(equalTo("Hello")))
      }
    )
}
