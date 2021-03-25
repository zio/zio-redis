package zio.redis
import zio.clock.Clock
import zio.test.ZSpec

object TempSpec extends ListSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    listSuite.provideCustomLayerShared(RedisExecutor.test ++ Clock.live)
}
