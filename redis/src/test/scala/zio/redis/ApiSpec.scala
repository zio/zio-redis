package zio.redis

import zio.clock.Clock
import zio.test._

object ApiSpec extends KeysSpec with ListSpec with SetsSpec with StringsSpec with GeoSpec with HyperLogLogSpec {

  def spec =
    suite("Redis commands")(
      keysSuite,
      listSuite,
      setsSuite,
      stringsSuite,
      geoSuite,
      hyperLogLogSuite
    ).provideCustomLayerShared(Executor ++ Clock.live)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
