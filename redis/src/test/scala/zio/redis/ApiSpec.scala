package zio.redis

import zio.test._
import zio.clock.Clock

object ApiSpec
    extends KeysSpec
    with ListSpec
    with SetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec {

  def spec =
    suite("Redis commands")(
      keysSuite,
      listSuite,
      setsSuite,
      stringsSuite,
      geoSuite,
      hyperLogLogSuite,
      hashSuite
    ).provideCustomLayerShared(Executor ++ Clock.live)

  private val Executor = RedisExecutor.live("127.0.0.1", 6379).orDie
}
