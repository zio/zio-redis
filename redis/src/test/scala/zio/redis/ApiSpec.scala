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
    with ScriptingSpec
    with ClusterSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Redis commands")(clusterSuite, singleNodeSuite) @@ sequential @@ withLiveEnvironment

  private val singleNodeSuite =
    suite("Single node executor")(
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
    ).provideShared(
      RedisExecutor.local,
      Redis.layer,
      ZLayer.succeed(codec)
    )

  private val clusterSuite =
    suite("Cluster executor")(
      connectionSuite,
      keysSuite,
      listSuite,
      stringsSuite,
      hashSuite,
      setsSuite,
      sortedSetsSuite,
      hyperLogLogSuite,
      geoSuite,
      clusterSpec
    ).provideShared(
      ClusterExecutor.layer,
      Redis.layer,
      ZLayer.succeed(codec),
      ZLayer.succeed(RedisClusterConfig(Chunk(RedisUri("localhost", 5000))))
    )
}
