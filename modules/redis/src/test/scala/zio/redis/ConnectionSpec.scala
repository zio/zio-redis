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
      suite("clientCaching")(
        test("track keys") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            _            <- redis.clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptIn))
            _            <- redis.clientCaching(true)
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo.flags.caching)(isSome(isTrue))
        },
        test("don't track keys") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            _            <- redis.clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptOut))
            _            <- redis.clientCaching(false)
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo.flags.caching)(isSome(isFalse))
        }
      ),
      suite("clientId")(
        test("get client id") {
          for {
            id <- ZIO.serviceWithZIO[Redis](_.clientId)
          } yield assert(id)(isGreaterThan(0L))
        }
      ),
      suite("clientInfo")(
        test("get client info") {
          for {
            redis <- ZIO.service[Redis]
            id    <- redis.clientId
            info  <- ZIO.serviceWithZIO[Redis](_.clientInfo)
          } yield assert(info.id)(isSome(equalTo(id))) && assert(info.name)(isNone)
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
      suite("clientList")(
        test("get clients' info") {
          for {
            info <- ZIO.serviceWithZIO[Redis](_.clientList())
          } yield assert(info)(isNonEmpty)
        },
        test("get clients' info filtered by type") {
          for {
            redis      <- ZIO.service[Redis]
            infoNormal <- redis.clientList(Some(ClientType.Normal))
            infoPubSub <- redis.clientList(Some(ClientType.PubSub))
          } yield assert(infoNormal)(isNonEmpty) && assert(infoPubSub)(isEmpty)
        },
        test("get clients' info filtered by client IDs") {
          for {
            redis           <- ZIO.service[Redis]
            id              <- redis.clientId
            nonExistingId    = id + 1
            info            <- redis.clientList(clientIds = Some((id, Nil)))
            infoNonExisting <- redis.clientList(clientIds = Some((nonExistingId, Nil)))
          } yield assert(info)(isNonEmpty) && assert(info.head.id)(isSome(equalTo(id))) && assert(infoNonExisting)(
            isEmpty
          )
        }
      ),
      suite("clientGetRedir")(
        test("tracking disabled") {
          for {
            redis <- ZIO.service[Redis]
            _     <- redis.clientTrackingOff
            redir <- redis.clientGetRedir
          } yield assert(redir)(equalTo(ClientTrackingRedirect.NotEnabled))
        },
        test("tracking enabled but not redirecting") {
          for {
            redis <- ZIO.service[Redis]
            _     <- redis.clientTrackingOn()
            redir <- redis.clientGetRedir
          } yield assert(redir)(equalTo(ClientTrackingRedirect.NotRedirected))
        }
      ),
      suite("client pause and unpause")(
        test("clientPause") {
          for {
            unit <- ZIO.serviceWithZIO[Redis](_.clientPause(1.second, Some(ClientPauseMode.All)))
          } yield assert(unit)(isUnit)
        },
        test("clientUnpause") {
          for {
            unit <- ZIO.serviceWithZIO[Redis](_.clientUnpause)
          } yield assert(unit)(isUnit)
        }
      ),
      test("set and get name") {
        for {
          redis <- ZIO.service[Redis]
          _     <- redis.clientSetName("foo")
          name  <- redis.clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      } @@ clusterExecutorUnsupported,
      suite("clientTracking")(
        test("enable tracking in broadcast mode and with prefixes") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            _            <- redis.clientTrackingOn(None, Some(ClientTrackingMode.Broadcast), prefixes = Set("foo"))
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo.redirect)(equalTo(ClientTrackingRedirect.NotRedirected)) &&
            assert(trackingInfo.flags)(
              equalTo(ClientTrackingFlags(clientSideCaching = true, trackingMode = Some(ClientTrackingMode.Broadcast)))
            ) &&
            assert(trackingInfo.prefixes)(equalTo(Set("foo")))
        },
        test("disable tracking") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo.redirect)(equalTo(ClientTrackingRedirect.NotEnabled)) &&
            assert(trackingInfo.flags)(
              equalTo(ClientTrackingFlags(clientSideCaching = false))
            ) &&
            assert(trackingInfo.prefixes)(equalTo(Set.empty[String]))
        }
      ),
      suite("clientTrackingInfo")(
        test("get tracking info when tracking is disabled") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo)(
            equalTo(
              ClientTrackingInfo(
                flags = ClientTrackingFlags(clientSideCaching = false),
                redirect = ClientTrackingRedirect.NotEnabled
              )
            )
          )
        },
        test("get tracking info when tracking is enabled in optin mode with noloop and caching on") {
          for {
            redis        <- ZIO.service[Redis]
            _            <- redis.clientTrackingOff
            _            <- redis.clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptIn), noLoop = true)
            _            <- redis.clientCaching(true)
            trackingInfo <- redis.clientTrackingInfo
          } yield assert(trackingInfo)(
            equalTo(
              ClientTrackingInfo(
                flags = ClientTrackingFlags(
                  clientSideCaching = true,
                  trackingMode = Some(ClientTrackingMode.OptIn),
                  caching = Some(true),
                  noLoop = true
                ),
                redirect = ClientTrackingRedirect.NotRedirected
              )
            )
          )
        }
      ),
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
      ),
      test("reset") {
        for {
          unit <- ZIO.serviceWithZIO[Redis](_.reset)
        } yield assert(unit)(isUnit)
      } @@ clusterExecutorUnsupported
    ) @@ sequential
}
