package zio.redis.strings

import java.util.concurrent.TimeUnit

import cats.instances.list._
import cats.syntax.foldable._
import io.chrisdavenport.rediculous.{RedisCommands, RedisIO}
import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class GetBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(ZIO.foreach_(items)(i => set(i, i)))
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{all => cmd, _}
    unsafeRun[LaserDiscClient](c => items.traverse_(i => c.send(cmd.get[String](Key.unsafeFrom(i)))))
  }

  @Benchmark
  def rediculous(): Unit =
    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.get[RedisIO](i).run(c)))

  @Benchmark
  def redis4cats(): Unit =
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.get(i)))

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(get(_).returning[String]))
}
