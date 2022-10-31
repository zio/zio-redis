package zio.redis

import zio._
import zio.test.TestAspect._
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
    with StreamsSpec
    with ScriptingSpec {

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
        streamsSuite,
        scriptingSpec
      ).provideLayerShared(LiveLayer) @@ sequential @@ withLiveEnvironment,
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
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(BaseSpec.TestExecutorUnsupported))
        .get
        .provideLayer(TestLayer)
    )

  private val LiveLayer =
    ZLayer.make[Redis](RedisExecutor.local, RedisLive.layer, ZLayer.succeed(codec))

  private val TestLayer =
    ZLayer.make[Redis](RedisExecutor.test, RedisLive.layer, ZLayer.succeed(codec))
}
