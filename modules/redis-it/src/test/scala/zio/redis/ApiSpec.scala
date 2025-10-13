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
          service(IntegrationSpec.SingleNode0, ".*Ready to accept connections.*"),
          service(IntegrationSpec.SingleNode1, ".*Ready to accept connections.*"),
          service(IntegrationSpec.MasterNode, ".*Cluster state changed: ok.*")
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
    ).filterNotTags(_.contains(IntegrationSpec.ClusterExecutorUnsupported))
      .getOrElse(Spec.empty) @@ flaky @@ ifEnvNotSet("CI")

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
      singleNodeConfig(IntegrationSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier)
    )
}
