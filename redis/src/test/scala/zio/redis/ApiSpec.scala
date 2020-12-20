package zio.redis

import zio.blocking.Blocking
import zio.clock.Clock
import zio.logging.Logging
import zio.random.Random
import zio.test._
import zio.test.environment.{ Live, TestClock, TestConsole, TestRandom, TestSystem }
import zio.{ Has, ULayer, ZLayer }

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
    with StreamsSpec {

  def spec: Spec[Has[Annotations.Service] with Has[Live.Service] with Has[Sized.Service] with Has[
    TestClock.Service
  ] with Has[TestConfig.Service] with Has[TestConsole.Service] with Has[TestRandom.Service] with Has[
    TestSystem.Service
  ] with Has[Clock.Service] with Has[zio.console.Console.Service] with Has[zio.system.System.Service] with Has[
    Random.Service
  ] with Has[Blocking.Service], TestFailure[java.io.Serializable], TestSuccess] =
    suite("Redis commands")(
      suite("Live Executor")(
        connectionSuite,
        keysSuite,
        listSuite,
        setsSuite,
        sortedSetsSuite,
        stringsSuite,
        geoSuite,
        hyperLogLogSuite,
        hashSuite,
        streamsSuite
      ).provideCustomLayerShared(
        (Logging.ignore >>> RedisExecutor.local.orDie) ++ Clock.live
      ),
      suite("Test Executor")(
        connectionSuite,
        setsSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(TestExecutorUnsupportedTag))
        .get
        .provideCustomLayerShared(RedisExecutor.test)
    )

  // Some groups of commands, such as keys, have functions that work across multiple
  // Redis instances, so we create a second one that can be provided as needed.
  val secondConfigLayer: ULayer[Has[RedisConfig]] = ZLayer.succeed(RedisConfig("localhost", 6380))
  val secondRedisService: ZLayer[Any with Any, RedisError.IOError, RedisExecutor] =
    (Logging.ignore ++ secondConfigLayer >>> RedisExecutor.live).fresh
}
