package zio.redis.lists

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, del, lRange, rPush }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class LRangeBenchmarks extends BenchmarkRuntime {
  @Param(Array("400"))
  var count: Int = _

  private var items: List[Int] = _

  private val key = "test-list"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList
    zioUnsafeRun(rPush(key, items.head, items.tail: _*).unit)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    zioUnsafeRun(del(key).unit)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(i => c.send(cmd.lrange[String](Key.unsafeFrom(key), Index(0L), Index.unsafeFrom(i.toLong))))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._

    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.lrange[RedisIO](key, 0L, i.toLong).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.lRange(key, 0L, i.toLong)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => lRange[String, String](key, 0 to i)))
}
