package zio.redis

import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

import java.net.InetAddress

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
      suite("clientKill")(
        test("error when a connection with the specified address doesn't exist") {
          for {
            error <- ZIO.serviceWithZIO[Redis](_.clientKill(Address(InetAddress.getByName("0.0.0.0"), 0)).either)
          } yield assert(error)(isLeft)
        },
        test("specify filters that don't kill the connection") {
          for {
            clientsKilled <-
              ZIO.serviceWithZIO[Redis](_.clientKill(ClientKillFilter.SkipMe(false), ClientKillFilter.Id(3341L)))
          } yield assert(clientsKilled)(equalTo(0L))
        },
        test("specify filters that kill the connection but skipme is enabled") {
          for {
            redis         <- ZIO.service[Redis]
            id            <- redis.clientId
            clientsKilled <- redis.clientKill(ClientKillFilter.SkipMe(true), ClientKillFilter.Id(id))
          } yield assert(clientsKilled)(equalTo(0L))
        }
      ),
      test("set and get name") {
        for {
          redis <- ZIO.service[Redis]
          _     <- redis.clientSetName("foo")
          name  <- redis.clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      } @@ clusterExecutorUnsupported,
      suite("clientUnblock")(
        test("unblock client that isn't blocked") {
          for {
            redis <- ZIO.service[Redis]
            id    <- redis.clientId
            bool  <- redis.clientUnblock(id)
          } yield assert(bool)(equalTo(false))
        }
      ),
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
