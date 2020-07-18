package zio.redis

import zio.test._
import zio.clock.Clock

object ApiSpec extends KeysSpec with ListSpec with GeoSpec with HyperLogLogSpec {

  def spec =
    suite("Redis commands")(
      keysSuite,
      listSuite,
      geoSuite,
      hyperLogLogSuite
    ).provideCustomLayerShared(Executor ++ Clock.live)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
