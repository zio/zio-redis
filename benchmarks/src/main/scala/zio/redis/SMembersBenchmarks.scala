package zio.redis

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO

/*
 * Baseline:
 * 
 * Benchmark                      (size)   Mode  Cnt  Score   Error  Units
 * SMembersBenchmarks.laserdisc      500  thrpt   15  1.534 ± 0.060  ops/s
 * SMembersBenchmarks.rediculous     500  thrpt   15  3.141 ± 0.013  ops/s
 * SMembersBenchmarks.redis4cats     500  thrpt   15  4.766 ± 0.064  ops/s
 * SMembersBenchmarks.zio            500  thrpt   15  3.401 ± 0.616  ops/s
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SMembersBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  private var size: Int = _

  private var items: List[String] = _

  private val key = "test-set"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to size).toList.map(_.toString)
    zioUnsafeRun(sAdd(key, items.head, items.tail: _*).unit)
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c => items.traverse_(_ => c.send(cmd.smembers(Key.unsafeFrom(key)))))
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(_ => RedisCommands.smembers[RedisIO](key).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(_ => c.sMembers(key)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(_ => sMembers(key)))
}
