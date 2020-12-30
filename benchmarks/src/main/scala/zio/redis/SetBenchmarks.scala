package zio.redis

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO

/*
 * Baseline:
 * 
 * Benchmark                 (count)   Mode  Cnt   Score   Error  Units
 * SetBenchmarks.laserdisc       500  thrpt   15   6.704 ± 0.604  ops/s
 * SetBenchmarks.rediculous      500  thrpt   15   6.719 ± 0.612  ops/s
 * SetBenchmarks.redis4cats      500  thrpt   15  15.129 ± 0.235  ops/s
 * SetBenchmarks.zio             500  thrpt   15  12.189 ± 0.123  ops/s
 * 
 * After 1st iteration:
 * 
 * Benchmark          (count)   Mode  Cnt   Score   Error  Units
 * SetBenchmarks.zio      500  thrpt   15  14.077 ± 0.757  ops/s
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SetBenchmarks extends BenchmarkRuntime {

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit =
    items = (0 to count).toList.map(_.toString)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c => items.traverse_(i => c.send(cmd.set(Key.unsafeFrom(i), i))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.set[RedisIO](i, i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.set(i, i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => set(i, i)))
}
