package zio.redis

import zio.{Chunk, ZLayer}
import zio.redis.executor.RedisExecutor
import zio.redis.executor.cluster.{RedisClusterConfig, RedisClusterExecutorLive}
import zio.test._
import zio.test.TestAspect._

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
      Node.Suite.provideCustomLayerShared(Node.Layer) @@ sequential @@ withLiveEnvironment,
      Cluster.Suite.provideCustomLayerShared(Cluster.Layer) @@ sequential @@ withLiveEnvironment,
      Test.Suite.provideCustomLayer(Test.Layer)
    )

  private object Node {
    val Suite: Spec[Redis with Annotations with Live with Sized with TestConfig, Any] =
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

    val Layer = {
      val executor = RedisExecutor.local.orDie
      val redis    = executor ++ ZLayer.succeed(codec) >>> RedisLive.layer
      redis
    }
  }

  private object Test {
    val Suite: Spec[Redis with Annotations with Live with Sized with TestConfig, Any] =
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

    val Layer = {
      val redis = RedisExecutor.test ++ ZLayer.succeed(codec) >>> RedisLive.layer
      redis
    }
  }

  private object Cluster {
    val Suite: Spec[Redis with Annotations with Live with Sized with TestConfig, Any] =
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

    val Layer = {
      val address       = RedisUri("localhost", 5000)
      val clusterConfig = ZLayer.succeed(RedisClusterConfig(Chunk(address)))
      val executor      = clusterConfig >>> RedisClusterExecutorLive.layer
      val redis         = executor ++ ZLayer.succeed(codec) >>> RedisLive.layer
      redis
    }
  }

}
