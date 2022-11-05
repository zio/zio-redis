package zio.redis

import zio.test.TestAspect._
import zio.test._
import zio.{Chunk, Layer, ZLayer}

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
    with ScriptingSpec
    with ClusterSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Redis commands")(
      Node.Suite.provideLayerShared(Node.Layer) @@ sequential @@ withLiveEnvironment,
      Cluster.Suite.provideLayerShared(Cluster.Layer) @@ sequential @@ withLiveEnvironment,
      Test.Suite.provideLayer(Test.Layer)
    )

  private object Node {
    val Suite: Spec[Redis, Any] =
      suite("Node Executor")(
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
      )

    val Layer: Layer[Any, Redis] = ZLayer.make[Redis](RedisExecutor.local.orDie, ZLayer.succeed(codec), RedisLive.layer)
  }

  private object Test {
    val Suite: Spec[Redis, Any] =
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
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(BaseSpec.TestExecutorUnsupported)).get

    val Layer: Layer[Any, Redis] = ZLayer.make[Redis](RedisExecutor.test, ZLayer.succeed(codec), RedisLive.layer)
  }

  private object Cluster {
    val Suite: Spec[Redis, Any] =
      suite("Cluster Executor")(
        connectionSuite,
        keysSuite,
        listSuite,
        stringsSuite,
        hashSuite,
        setsSuite,
        sortedSetsSuite,
        hyperLogLogSuite,
        geoSuite,
        streamsSuite @@ clusterExecutorUnsupported,
        scriptingSpec @@ clusterExecutorUnsupported,
        clusterSpec
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(BaseSpec.ClusterExecutorUnsupported)).get

    val Layer: Layer[Any, Redis] =
      ZLayer.make[Redis](
        ZLayer.succeed(RedisClusterConfig(Chunk(RedisUri("localhost", 5000)))),
        ClusterExecutor.layer,
        ZLayer.succeed(codec),
        RedisLive.layer
      )
  }

}
