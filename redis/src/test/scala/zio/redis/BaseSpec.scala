package zio.redis

import zio.UIO
import zio.duration._
import zio.random.Random
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec
import zio.test.TestAspect.tag
import zio.test._
import zio.test.environment.Live

import java.time.Instant
import java.util.UUID

trait BaseSpec extends DefaultRunnableSpec {
  override def aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(60.seconds))

  def instantOf(millis: Long): UIO[Instant] = UIO(Instant.now().plusMillis(millis))

  implicit val codec: Codec = StringUtf8Codec

  val uuid: UIO[String] = UIO(UUID.randomUUID().toString)

  val TestExecutorUnsupportedTag                   = "test executor unsupported"
  lazy val testExecutorUnsupported: TestAspectPoly = tag(TestExecutorUnsupportedTag)

  val genStringRedisTypeOption: Gen[Random, Option[RedisType]] =
    Gen.option(Gen.constSample(Sample.noShrink(RedisType.String)))
  val genCountOption: Gen[Random, Option[Count]]    = Gen.option(Gen.long(0, 100000).map(Count))
  val genPatternOption: Gen[Random, Option[String]] = Gen.option(Gen.constSample(Sample.noShrink("*")))
}
