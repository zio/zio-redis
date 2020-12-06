package zio.redis

//import zio.{Chunk, Has, ZLayer}
import zio.clock.Clock
import zio.logging.Logging
import zio.test._
import SecondRedisExecutorLayer._

object ApiSpec
    extends ConnectionSpec
    with KeysSpec
    with ListSpec
    with SetsSpec
    with SortedSetsSpec
    with StringsSpec
    with GeoSpec
    with HyperLogLogSpec
    with HashSpec {

  def spec =
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
        hashSuite
      ).provideCustomLayerShared(Logging.ignore >>> Executor ++ Clock.live ++ SecondExecutor),
      suite("Test Executor")(
        connectionSuite,
        setsSuite
      ).filterAnnotations(TestAnnotation.tagged)(t => !t.contains(TestExecutorUnsupportedTag))
        .get
        .provideCustomLayerShared(RedisExecutor.test)
    )

  val Executor = RedisExecutor.loopback(6379).orDie

  val SecondExecutor = SecondRedisExecutor.loopback(6380).orDie
}
