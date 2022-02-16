package zio.redis

import zio.ZLayer
import zio.clock.Clock
import zio.logging.Logging
import zio.test.TestAspect._
import zio.test._
import zio.test.environment._

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
    with StreamsSpec
    with ScriptingSpec {

  def spec: ZSpec[TestEnvironment, Failure] =
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
        streamsSuite,
        scriptingSpec
      ).provideCustomLayerShared((Logging.ignore ++ ZLayer.succeed(codec) >>> RedisExecutor.local.orDie) ++ Clock.live)
        @@ sequential,
      suite("Test Executor")(
        connectionSuite,
        keysSuite,
        setsSuite,
        hyperLogLogSuite,
        listSuite,
        hashSuite,
        sortedSetsSuite,
        geoSuite,
        stringsSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(TestExecutorUnsupportedTag))
        .get
        .provideSomeLayer[TestEnvironment](RedisExecutor.test)
    )
}
