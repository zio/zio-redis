package zio.redis

import zio.ZLayer
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

  // scalafix:off
  def spec =
    // scalafix:on
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
      ).provideCustomLayerShared((Logging.ignore ++ ZLayer.succeed(codec) >>> RedisExecutor.local.orDie) ++ Clock.live) @@ TestAspect.ignore,
      suite("Test Executor")(
        connectionSuite @@ TestAspect.ignore,
        setsSuite @@ TestAspect.ignore,
        hyperLogLogSuite @@ TestAspect.ignore,
        listSuite @@ TestAspect.ignore,
        hashSuite @@ TestAspect.ignore,
        sortedSetsSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(TestExecutorUnsupportedTag))
        .get
        .provideCustomLayerShared(RedisExecutor.test ++ Clock.live)
    )
}
