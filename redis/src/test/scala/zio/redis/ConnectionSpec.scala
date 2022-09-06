package zio.redis

import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

import java.net.InetAddress

trait ConnectionSpec extends BaseSpec {
  def connectionSuite: Spec[Redis, RedisError] =
    suite("connection")(
      suite("clientCaching")(
        test("track keys") {
          for {
            _            <- clientTrackingOff
            _            <- clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptIn))
            _            <- clientCaching(true)
            trackingInfo <- clientTrackingInfo
          } yield assert(trackingInfo.flags.caching)(isSome(isTrue))
        },
        test("don't track keys") {
          for {
            _            <- clientTrackingOff
            _            <- clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptOut))
            _            <- clientCaching(false)
            trackingInfo <- clientTrackingInfo
          } yield assert(trackingInfo.flags.caching)(isSome(isFalse))
        }
      ),
      suite("clientId")(
        test("get client id") {
          for {
            id <- clientId
          } yield assert(id)(isGreaterThan(0L))
        }
      ),
      suite("clientInfo")(
        test("encode client info") {
          val t =
            "id=25 addr=127.0.0.1:49538 laddr=127.0.0.1:6379 fd=8 name= age=3435 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=22298 events=r cmd=client|info user=default redir=-1 resp=2"
          val r =
            for {
              info <- ClientInfo.decode(t)
              t2    = ClientInfo.encode(info)
            } yield assert(t2)(equalTo(t))

          assert(r.isRight)(isTrue)
        },
        test("parse client info") {
          val t =
            "id=25 addr=127.0.0.1:49538 laddr=127.0.0.1:6379 fd=8 name= age=3435 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=22298 events=r cmd=client|info user=default redir=-1 resp=2"
          val r =
            for {
              info <- ClientInfo.decode(t)
            } yield assert(info.id)(equalTo(25L)) &&
              assert(info.age)(equalTo(Some(java.time.Duration.ofSeconds(3435))))

          assert(r.isRight)(isTrue)
        },
        test("parse client info with unknown field") {
          val t =
            "id=25 addr=127.0.0.1:49538 new-field=? laddr=127.0.0.1:6379 fd=8 name= age=3435 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=22298 events=r cmd=client|info user=default redir=-1 resp=2"
          val r =
            for {
              info <- ClientInfo.decode(t)
            } yield assert(info.id)(equalTo(25L)) &&
              assert(info.age)(equalTo(Some(java.time.Duration.ofSeconds(3435))))

          assert(r.isRight)(isTrue)
        },
        test("parse client info with unknown clientFlag") {
          val t =
            "id=25 addr=127.0.0.1:49538 new-field=? laddr=127.0.0.1:6379 fd=8 name= age=3435 idle=0 flags=Na db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=22298 events=r cmd=client|info user=default redir=-1 resp=2"
          val r =
            for {
              info <- ClientInfo.decode(t)
            } yield assert(info.id)(equalTo(25L)) &&
              assert(info.age)(equalTo(Some(java.time.Duration.ofSeconds(3435))))

          assert(r.isRight)(isTrue)
        },
        test("get client info") {
          for {
            info <- clientInfo
          } yield assert(info.id)(isGreaterThan(0L))
        }
      ),
      suite("clientList")(
        test("returns non empty list of clients") {
          for {
            clients <- clientList()
          } yield assert(clients)(isNonEmpty)
        },
        test("zero results when id does not exists") {
          for {
            res <- clientList(ClientListFilter.Id(1))
          } yield assert(res)(isEmpty)
        }
      ),
      suite("clientKill")(
        test("error when a connection with the specifed address doesn't exist") {
          for {
            error <- clientKill(Address(InetAddress.getByName("0.0.0.0"), 0)).either
          } yield assert(error)(isLeft)
        },
        test("specify filters that don't kill the connection") {
          for {
            clientsKilled <- clientKill(ClientKillFilter.SkipMe(false), ClientKillFilter.Id(3341L))
          } yield assert(clientsKilled)(equalTo(0L))
        },
        test("specify filters that kill the connection but skipme is enabled") {
          for {
            id            <- clientId
            clientsKilled <- clientKill(ClientKillFilter.SkipMe(true), ClientKillFilter.Id(id))
          } yield assert(clientsKilled)(equalTo(0L))
        }
      ),
      suite("clientGetRedir")(
        test("tracking disabled") {
          for {
            _     <- clientTrackingOff
            redir <- clientGetRedir
          } yield assert(redir)(equalTo(ClientTrackingRedirect.NotEnabled))
        },
        test("tracking enabled but not redirecting") {
          for {
            _     <- clientTrackingOn()
            redir <- clientGetRedir
          } yield assert(redir)(equalTo(ClientTrackingRedirect.NotRedirected))
        }
      ),
      suite("client pause and unpause")(
        test("clientPause") {
          for {
            unit <- clientPause(1.second, Some(ClientPauseMode.All))
          } yield assert(unit)(isUnit)
        },
        test("clientUnpause") {
          for {
            unit <- clientUnpause
          } yield assert(unit)(isUnit)
        }
      ),
      test("set and get name") {
        for {
          _    <- clientSetName("foo")
          name <- clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      },
      suite("clientTracking")(
        test("enable tracking in broadcast mode and with prefixes") {
          for {
            _            <- clientTrackingOff
            _            <- clientTrackingOn(None, Some(ClientTrackingMode.Broadcast), prefixes = Set("foo"))
            trackingInfo <- clientTrackingInfo
          } yield assert(trackingInfo.redirect)(equalTo(ClientTrackingRedirect.NotRedirected)) &&
            assert(trackingInfo.flags)(
              equalTo(ClientTrackingFlags(clientSideCaching = true, trackingMode = Some(ClientTrackingMode.Broadcast)))
            ) &&
            assert(trackingInfo.prefixes)(equalTo(Set("foo")))
        },
        test("disable tracking") {
          for {
            _            <- clientTrackingOff
            trackingInfo <- clientTrackingInfo
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
            _            <- clientTrackingOff
            trackingInfo <- clientTrackingInfo
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
            _            <- clientTrackingOff
            _            <- clientTrackingOn(trackingMode = Some(ClientTrackingMode.OptIn), noLoop = true)
            _            <- clientCaching(true)
            trackingInfo <- clientTrackingInfo
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
            id   <- clientId
            bool <- clientUnblock(id)
          } yield assert(bool)(equalTo(false))
        }
      ),
      suite("ping")(
        test("PING with no input") {
          ping(None).map(assert(_)(equalTo("PONG")))
        },
        test("PING with input") {
          ping(Some("Hello")).map(assert(_)(equalTo("Hello")))
        },
        test("PING with a string argument will not lock executor") {
          ping(Some("Hello with a newline\n")).map(assert(_)(equalTo("Hello with a newline\n")))
        },
        test("PING with a multiline string argument will not lock executor") {
          ping(Some("Hello with a newline\r\nAnd another line\n"))
            .map(assert(_)(equalTo("Hello with a newline\r\nAnd another line\n")))
        }
      ),
      test("reset") {
        for {
          unit <- reset
        } yield assert(unit)(isUnit)
      }
    ) @@ sequential
}
