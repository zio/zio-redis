package zio.redis

import zio.test.TestAspect._
import zio.test._
import zio.{Clock, Random, Scope, ZLayer}

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

  def spec: Spec[TestEnvironment with Scope, Any] =
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
      ).provideCustomLayer(LiveLayer) @@ sequential @@ withLiveEnvironment,
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
        .provideSomeLayer[TestEnvironment](TestLayer)
    )

  private val LiveLayer = {
    val executor = RedisExecutor.local.orDie
    val redis    = executor ++ ZLayer.succeed(codec) >>> Redis.live
    redis ++ liveEnvironment

  }

  private val TestLayer = {
    val redis = RedisExecutor.test ++ ZLayer.succeed(codec) >>> Redis.live
    ZLayer.make[Redis with Random with Clock](
      redis,
      ZLayer.fromZIO(testRandom),
      ZLayer.fromZIO(testClock)
    )
  }
}
