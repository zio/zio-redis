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
    suite("Redis commands")(singleNodeSuite) @@ sequential @@ withLiveEnvironment

  private val singleNodeSuite =
    suite("Single node executor")(
      connectionSuite
//      keysSuite,
//      listSuite,
//      setsSuite,
//      sortedSetsSuite,
//      stringsSuite,
//      geoSuite,
//      hyperLogLogSuite,
//      hashSuite,
//      streamsSuite,
//      scriptingSpec
    ).provideShared(
      SingleNodeExecutor.local,
      Redis.layer,
      ZLayer.succeed(ProtobufCodecSupplier)
    )

//  private val clusterSuite =
//    suite("Cluster executor")(
//      connectionSuite,
//      keysSuite,
//      listSuite,
//      stringsSuite,
//      hashSuite,
//      setsSuite,
//      sortedSetsSuite,
//      hyperLogLogSuite,
//      geoSuite,
//      streamsSuite,
//      scriptingSpec,
//      clusterSpec
//    ).provideShared(
//      ClusterExecutor.layer,
//      Redis.layer,
//      ZLayer.succeed(ProtobufCodecSupplier),
//      ZLayer.succeed(RedisClusterConfig(Chunk(RedisUri("localhost", 5000))))
//    ).filterNotTags(_.contains(BaseSpec.ClusterExecutorUnsupported))
//      .getOrElse(Spec.empty)
}
