package zio.redis

import java.time.Instant
import java.util.UUID

import zio.UIO
import zio.duration._
import zio.test._

trait BaseSpec extends DefaultRunnableSpec {
  override def aspects = List(TestAspect.timeout(60.seconds))

  val uuid                                  = UIO(UUID.randomUUID().toString)
  def instantOf(millis: Long): UIO[Instant] = UIO(Instant.now().plusMillis(millis))
}
