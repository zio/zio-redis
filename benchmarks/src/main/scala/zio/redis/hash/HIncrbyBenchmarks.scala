package zio.redis.hash

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{ BenchmarkRuntime, hIncrBy, hSet }

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class HIncrbyBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var size: Int = _

  @Param(Array("1"))
  var increment: Long = _

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
    import cats.implicits.toFoldableOps
    import _root_.laserdisc.{ all => cmd, _ }
    import cats.instances.list._
    unsafeRun[LaserDiscClient](c =>
      items.traverse_(it =>
        c.send(cmd.hincrby(Key.unsafeFrom(key), Key.unsafeFrom(it._1), NonZeroLong.unsafeFrom(increment)))
      )
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c =>
      items.traverse_(it => RedisCommands.hincrby[RedisIO](key, it._1, increment).run(c))
    )
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[Long]](c => items.traverse_(it => c.hIncrBy(key, it._1, increment)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(it => hIncrBy(key, it._1, increment)))
}
