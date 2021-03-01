package zio.redis.hash

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, hIncrByFloat, hSet }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class HIncrbyFloatBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  private var size: Int = _
  @Param(Array("1.0"))
  private var increment: Double = _

  private var items: List[(String, String)] = _

  private val key = "test-hash"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to size).map(e => e.toString -> e.toString).toList
    zioUnsafeRun(hSet(key, items.head, items.tail: _*).unit)
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.implicits.toFoldableOps
    import cats.instances.list._
    unsafeRun[LaserDiscClient](c =>
      items.traverse_(it =>
        c.send(cmd.hincrby(Key.unsafeFrom(key), Key.unsafeFrom(it._1), NonZeroDouble.unsafeFrom(increment)))
      )
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c =>
      items.traverse_(it => RedisCommands.hincrbyfloat[RedisIO](key, it._1, increment).run(c))
    )
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[Long]](c => items.traverse_(it => c.hIncrByFloat(key, it._1, increment)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(it => hIncrByFloat(key, it._1, increment)))
}
