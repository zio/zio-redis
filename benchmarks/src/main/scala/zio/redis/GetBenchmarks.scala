package zio.redis

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import zio.ZIO

import scala.util.Random

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(4)
class GetBenchmarks {
  import BenchmarkRuntime._
  import BenchmarksUtils._
  import RedisClients._

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(Random.nextString)
    zioUnsafeRun(ZIO.foreach_(items)(i => set(i, i, None, None, None)))
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeClientRun[LaserDiskClient](c => items.traverse_(i => c.send(cmd.get[String](Key.unsafeFrom(i)))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeClientRun[RediculousClient](c => items.traverse_(i => RedisCommands.get[RedisIO](i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeClientRun[Redis4CatsClient](c => items.traverse_(i => c.get(i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(get))
}
