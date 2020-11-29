package zio.redis

import zio.clock.Clock
import zio.logging.Logging
import zio.test._

object ApiSpec
    extends ConnectionSpec
    with KeysSpec
    with ListSpec
    with SetsSpec
    with SortedSetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec
    with StreamsSpec {

  def spec =
    suite("Redis commands")(
      suite("Live Executor")(
        connectionSuite,
        keysSuite,
        listSuite,
        setsSuite,
        sortedSetsSuite,
        stringsSuite,
        geoSuite,
        hyperLogLogSuite,
        hashSuite,
        streamsSuite
      ).provideCustomLayerShared(Logging.ignore >>> Executor ++ Clock.live),
      suite("Test Executor")(
        connectionSuite,
        setsSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(TestExecutorUnsupportedTag))
        .get
        .provideCustomLayerShared(RedisExecutor.test)
    )

  private val Executor = RedisExecutor.loopback().orDie
}
