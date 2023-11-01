package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
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
    with ClusterSpec
    with PubSubSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Redis commands")(ClusterSuite, SingleNodeSuite)
      .provideShared(
        compose(
          service(BaseSpec.SingleNode0, ".*Ready to accept connections.*"),
          service(BaseSpec.SingleNode1, ".*Ready to accept connections.*"),
          service(BaseSpec.MasterNode, ".*Cluster correctly created.*")
        )
      ) @@ sequential @@ withLiveEnvironment

  private final val ClusterSuite =
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
      streamsSuite,
      scriptingSpec,
      clusterSpec
    ).provideSomeShared[DockerComposeContainer](
      Redis.cluster,
      masterNodeConfig,
      ZLayer.succeed(ProtobufCodecSupplier)
    ).filterNotTags(_.contains(BaseSpec.ClusterExecutorUnsupported))
      .getOrElse(Spec.empty) @@ flaky(250)

  private final val SingleNodeSuite =
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
      scriptingSpec,
      pubSubSuite
    ).provideSomeShared[DockerComposeContainer](
      Redis.singleNode,
      RedisSubscription.singleNode,
      singleNodeConfig(BaseSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier)
    )
}
