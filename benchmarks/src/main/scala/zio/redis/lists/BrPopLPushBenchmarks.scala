package zio.redis.lists

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.duration._
import zio.redis.{BenchmarkRuntime, brPopLPush, del, rPush}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class BrPopLPushBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key = "test-list"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(rPush(key, items.head, items.tail: _*).unit)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    zioUnsafeRun(del(key).unit)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{all => cmd, _}
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(_ =>
        c.send(
          cmd.blocking.brpoplpush[String](Key.unsafeFrom(key), Key.unsafeFrom(key), PosInt.unsafeFrom(1))
        )
      )
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._

    unsafeRun[RediculousClient](c => items.traverse_(_ => RedisCommands.brpoplpush[RedisIO](key, key, 1).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    import scala.concurrent.duration._

    unsafeRun[Redis4CatsClient[String]](c =>
      items.traverse_(_ => c.brPopLPush(Duration(1, TimeUnit.SECONDS), key, key))
    )
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(
    ZIO.foreach_(items)(_ => brPopLPush[String, String, String](key, key, 1.second))
  )
}
