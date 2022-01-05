package zio.redis

import zio.ZLayer
import zio.clock.Clock
import zio.logging.Logging
import zio.test.TestAspect._
import zio.test._
import zio.test.environment._

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
    with ServerSpec {

  def spec: ZSpec[TestEnvironment, Failure] =
    suite("Redis commands")(
      suite("Live Executor")(
        serverSpec
      ).provideCustomLayerShared((Logging.ignore ++ ZLayer.succeed(codec) >>> RedisExecutor.local.orDie) ++ Clock.live)
        @@ sequential
    )
}
