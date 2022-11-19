package zio.redis

import zio._
import zio.schema.codec.{BinaryCodec, ProtobufCodec}
import zio.test.TestAspect.tag
import zio.test._

import java.time.Instant
import java.util.UUID

trait BaseSpec extends ZIOSpecDefault {
  implicit val codec: BinaryCodec = ProtobufCodec

  override def aspects: Chunk[TestAspectAtLeastR[Live]] =
    Chunk.succeed(TestAspect.timeout(10.seconds))

  def instantOf(millis: Long): UIO[Instant] = ZIO.succeed(Instant.now().plusMillis(millis))

  final val genStringRedisTypeOption: Gen[Any, Option[RedisType]] =
    Gen.option(Gen.constSample(Sample.noShrink(RedisType.String)))

  final val genCountOption: Gen[Any, Option[Count]] =
    Gen.option(Gen.long(0, 100000).map(Count(_)))

  final val genPatternOption: Gen[Any, Option[String]] =
    Gen.option(Gen.constSample(Sample.noShrink("*")))

  final val uuid: UIO[String] =
    ZIO.succeed(UUID.randomUUID().toString)

  final val testExecutorUnsupported: TestAspectPoly =
    tag(BaseSpec.TestExecutorUnsupported)

  /* TODO
   *  We can try to support the most unsupported commands for cluster with:
   *  - default connection for commands without a key and for multiple key commands with
   *    the limitation that all keys have to be in the same slot
   *  - fork/join approach for commands that operate on keys with different slots
   */
  final val clusterExecutorUnsupported: TestAspectPoly =
    tag(BaseSpec.ClusterExecutorUnsupported)
}

object BaseSpec {
  final val TestExecutorUnsupported    = "test executor unsupported"
  final val ClusterExecutorUnsupported = "cluster executor unsupported"
}
