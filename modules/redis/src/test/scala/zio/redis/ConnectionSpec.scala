package zio.redis

import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

trait ConnectionSpec extends BaseSpec {
  def connectionSuite: Spec[Redis, RedisError] =
    suite("connection")(
      suite("authenticating")(
        test("auth with 'default' username") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.auth("default", "password")
          } yield assert(res)(isUnit)
        }
      ),
      suite("clientId")(
        test("get client id") {
          for {
            id <- ZIO.serviceWithZIO[Redis](_.clientId)
          } yield assert(id)(isGreaterThan(0L))
        }
      ),
      test("set and get name") {
        for {
          redis <- ZIO.service[Redis]
          _     <- redis.clientSetName("foo")
          name  <- redis.clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      } @@ clusterExecutorUnsupported,
      suite("ping")(
        test("PING with no input") {
          ZIO.serviceWithZIO[Redis](_.ping(None).map(assert(_)(equalTo("PONG"))))
        } @@ clusterExecutorUnsupported,
        test("PING with input") {
          ZIO.serviceWithZIO[Redis](_.ping(Some("Hello")).map(assert(_)(equalTo("Hello"))))
        },
        test("PING with a string argument will not lock executor") {
          ZIO.serviceWithZIO[Redis](
            _.ping(Some("Hello with a newline\n")).map(assert(_)(equalTo("Hello with a newline\n")))
          )
        },
        test("PING with a multiline string argument will not lock executor") {
          ZIO.serviceWithZIO[Redis](
            _.ping(Some("Hello with a newline\r\nAnd another line\n"))
              .map(assert(_)(equalTo("Hello with a newline\r\nAnd another line\n")))
          )
        }
      )
    ) @@ sequential
}
