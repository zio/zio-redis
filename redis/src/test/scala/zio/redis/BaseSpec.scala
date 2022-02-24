package zio.redis

import zio.UIO
import zio.duration._
import zio.random.Random
import zio.schema.codec.{Codec, JsonCodec}
import zio.test.TestAspect.tag
import zio.test._
import zio.test.environment.Live

import java.time.Instant
import java.util.UUID

trait BaseSpec extends DefaultRunnableSpec {
  implicit val codec: Codec = JsonCodec

  override def aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(60.seconds))

  def instantOf(millis: Long): UIO[Instant] = UIO(Instant.now().plusMillis(millis))

  final val genStringRedisTypeOption: Gen[Random, Option[RedisType]] =
    Gen.option(Gen.constSample(Sample.noShrink(RedisType.String)))

  final val genCountOption: Gen[Random, Option[Count]] =
    Gen.option(Gen.long(0, 100000).map(Count))

  final val genPatternOption: Gen[Random, Option[String]] =
    Gen.option(Gen.constSample(Sample.noShrink("*")))

  final val uuid: UIO[String] =
    UIO(UUID.randomUUID().toString)

  final val testExecutorUnsupported: TestAspectPoly =
    tag(BaseSpec.TestExecutorUnsupported)
}

object BaseSpec {
  final val TestExecutorUnsupported = "test executor unsupported"
}
