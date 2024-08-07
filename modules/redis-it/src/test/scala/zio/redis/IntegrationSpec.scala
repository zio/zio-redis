package zio.redis

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import org.testcontainers.containers.wait.strategy.Wait
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}
import zio.test.TestAspect.{fibers, silentLogging, tag}
import zio.test._
import zio.testcontainers._
import zio.{ULayer, _}

import java.io.File
import java.util.UUID

trait IntegrationSpec extends ZIOSpecDefault {
  implicit def summonCodec[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec

  override def aspects: Chunk[TestAspectAtLeastR[Live]] =
    Chunk(fibers, silentLogging)

  final def compose(services: ExposedService*): ULayer[DockerComposeContainer] =
    ZLayer.fromTestContainer {
      DockerComposeContainer(
        new File(getClass.getResource("/docker-compose.yml").getFile),
        services.toList
      )
    }

  final def masterNodeConfig: URLayer[DockerComposeContainer, RedisClusterConfig] =
    ZLayer {
      for {
        docker      <- ZIO.service[DockerComposeContainer]
        hostAndPort <- docker.getHostAndPort(IntegrationSpec.MasterNode)(6379)
        uri          = RedisUri(hostAndPort._1, hostAndPort._2)
      } yield RedisClusterConfig(Chunk(uri))
    }

  final def service(name: String, waitMessage: String): ExposedService =
    ExposedService(name, 6379, Wait.forLogMessage(waitMessage, 1))

  final def singleNodeConfig(host: String): URLayer[DockerComposeContainer, RedisConfig] =
    ZLayer {
      for {
        docker      <- ZIO.service[DockerComposeContainer]
        hostAndPort <- docker.getHostAndPort(host)(6379)
      } yield RedisConfig(hostAndPort._1, hostAndPort._2)
    }

  /* TODO
   *  We can try to support the most unsupported commands for cluster with:
   *  - [DONE] default connection for commands without a key and for multiple key commands with
   *    the limitation that all keys have to be in the same slot
   *  - fork/join approach for commands that operate on keys with different slots
   */
  final val clusterExecutorUnsupported: TestAspectPoly =
    tag(IntegrationSpec.ClusterExecutorUnsupported)

  final val genStringRedisTypeOption: Gen[Any, Option[RedisType]] =
    Gen.option(Gen.constSample(Sample.noShrink(RedisType.String)))

  final val genCountOption: Gen[Any, Option[Count]] =
    Gen.option(Gen.long(0, 100000).map(Count(_)))

  final val genPatternOption: Gen[Any, Option[String]] =
    Gen.option(Gen.constSample(Sample.noShrink("*")))

  final val uuid: UIO[String] =
    ZIO.succeed(UUID.randomUUID().toString)
}

object IntegrationSpec {
  final val ClusterExecutorUnsupported = "cluster executor not supported"
  final val MasterNode                 = "cluster-node5"
  final val SingleNode0                = "single-node0"
  final val SingleNode1                = "single-node1"
}
