package zio.redis.lists

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, del, lPushX, rPush }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class LPushXBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key = "test-list"

  @Setup(Level.Invocation)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(rPush(key, items.head).unit)
  }

  @TearDown(Level.Invocation)
  def tearDown(): Unit =
    zioUnsafeRun(del(key).unit)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c => items.traverse_(i => c.send(cmd.lpushx[String](Key.unsafeFrom(key), i))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._

    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.lpushx[RedisIO](key, i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.lPushX(key, i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => lPushX[String, String](key, i)))
}
