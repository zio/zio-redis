package zio.redis

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO

/*
 * Baseline:
 *
 * GetBenchmarks.laserdisc       500  thrpt   15   6.929 ± 0.524  ops/s
 * GetBenchmarks.rediculous      500  thrpt   15   6.532 ± 0.600  ops/s
 * GetBenchmarks.redis4cats      500  thrpt   15  13.558 ± 1.124  ops/s
 * GetBenchmarks.zio             500  thrpt   15   9.430 ± 0.812  ops/s
 *
 * After 1st draft:
 *
 * Benchmark          (count)   Mode  Cnt   Score   Error  Units
 * GetBenchmarks.zio      500  thrpt   15  14.155 ± 0.204  ops/s
 * 
 * After specializing number parsing:
 * 
 * Benchmark          (count)   Mode  Cnt   Score   Error  Units
 * GetBenchmarks.zio      500  thrpt   15  14.575 ± 0.316  ops/s
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class GetBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(ZIO.foreach_(items)(i => set(i, i)))
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[LaserDiscClient](c => items.traverse_(i => c.send(cmd.get[String](Key.unsafeFrom(i)))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.get[RedisIO](i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.get(i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(get))
}
