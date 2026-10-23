package zio.redis.internal

import zio._
import zio.redis._
import zio.test.Assertion._
import zio.test._

import java.net.{InetSocketAddress, UnknownHostException}

object RedisConnectionSpec extends BaseSpec {
  override def spec: Spec[Environment, Any] =
    suite("RedisConnection")(
      suite("connect to a host name that does not resolve fails with UnknownHostException")(
        test("without TLS") {
          assertConnectFailsToUnresolvedAddress(ssl = false)
        },
        test("with TLS") {
          assertConnectFailsToUnresolvedAddress(ssl = true)
        }
      )
    )

  private def assertConnectFailsToUnresolvedAddress(ssl: Boolean) =
    for {
      error <- ZIO.scoped(RedisConnection.connect(unresolvedAddress, sni = None, ssl = ssl)).flip
    } yield assert(error)(isSubtype[UnknownHostException](hasMessage(equalTo(unresolvedAddress.getHostString))))

  private val unresolvedAddress = InetSocketAddress.createUnresolved("redis-host", 6379)
}
