package zio.redis

import zio.duration._
import zio.test.Assertion._
import zio.test._

trait ConnectionSpec extends BaseSpec {

  val connectionSuite: Spec[RedisExecutor, TestFailure[RedisError], TestSuccess] =
    suite("connection")(
      suite("clientCaching")(
        testM("track keys") {
          for {
            unit <- clientCaching(true)
          } yield assert(unit)(isUnit)
        },
        testM("don't track keys") {
          for {
            unit <- clientCaching(false)
          } yield assert(unit)(isUnit)
        }
      ),
      suite("client pausing")(
        testM("pause client") {
          for {
            unit <- clientPause(3.seconds, Some(ClientPauseMode.All))
          } yield assert(unit)(isUnit)
        },
        testM("unpause client") {
          for {
            unit <- clientUnpause
          } yield assert(unit)(isUnit)
        }
      ),
      suite("clientSetName")(
        testM("set name") {
          for {
            unit <- clientSetName("name")
          } yield assert(unit)(isUnit)
        }
      ),
      suite("clientTracking")(
        testM("enable tracking") {
          for {
            unit <- clientTrackingOn(Some(3L), Some(ClientTrackingMode.OptIn), prefixes = Set("foo"))
          } yield assert(unit)(isUnit)
        },
        testM("disable tracking") {
          for {
            unit <- clientTrackingOff
          } yield assert(unit)(isUnit)
        }
      ),
      suite("ping")(
        testM("PING with no input") {
          ping(None).map(assert(_)(equalTo("PONG")))
        },
        testM("PING with input") {
          ping(Some("Hello")).map(assert(_)(equalTo("Hello")))
        }
      ),
      suite("quit and reset")(
        testM("quit") {
          for {
            unit <- quit
          } yield assert(unit)(isUnit)
        },
        testM("reset") {
          for {
            unit <- reset
          } yield assert(unit)(isUnit)
        }
      )
    )
}
