package zio.redis.sets

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, sAdd, sDiffStore }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SDiffStoreBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(ZIO.foreach_(items)(i => sAdd(i, i)))
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(i => c.send(cmd.sdiffstore(Key.unsafeFrom(i), Key.unsafeFrom(i), Key.unsafeFrom(i))))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.sdiffstore[RedisIO](i, List(i, i)).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.sDiffStore(i, i, i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => sDiffStore(i, i, i)))
}
