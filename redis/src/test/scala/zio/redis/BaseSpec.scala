package zio.redis

import java.time.Instant
import java.util.UUID

import zio.UIO
import zio.duration._
import zio.test.TestAspect.tag
import zio.test._
import zio.test.environment.Live

trait BaseSpec extends DefaultRunnableSpec {
  override def aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(60.seconds))

  def instantOf(millis: Long): UIO[Instant] = UIO(Instant.now().plusMillis(millis))

  val uuid: UIO[String] = UIO(UUID.randomUUID().toString)

  val TestExecutorUnsupportedTag                   = "test executor unsupported"
  lazy val testExecutorUnsupported: TestAspectPoly = tag(TestExecutorUnsupportedTag)
}
