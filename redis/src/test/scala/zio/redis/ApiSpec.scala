package zio.redis

import zio.clock.Clock
import zio.test._
import zio.duration._

object ApiSpec
    extends KeysSpec
    with ListSpec
    with SetsSpec
    with SortedSetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec {

  def spec =
    suite("Redis commands")(
      keysSuite,
      listSuite,
      setsSuite,
      sortedSetsSuite,
      stringsSuite,
      geoSuite,
      hyperLogLogSuite,
      hashSuite
    ).provideCustomLayerShared(Executor ++ Clock.live)

  private val Executor = RedisExecutor.live(PoolConfig("127.0.0.1", 6379, 1, 10.seconds, 1, 10.seconds, 1, 10.seconds))
}
