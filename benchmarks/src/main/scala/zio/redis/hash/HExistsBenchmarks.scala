package zio.redis.hash

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis.{BenchmarkRuntime, hExists, hSet}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class HExistsBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var size: Int = _

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
    import _root_.laserdisc.{all => cmd, _}
    import cats.implicits.toFoldableOps
    import cats.instances.list._
    unsafeRun[LaserDiscClient](c =>
      items.traverse_(it => c.send(cmd.hexists(Key.unsafeFrom(key), Key.unsafeFrom(it._1))))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._
    unsafeRun[RediculousClient](c => items.traverse_(it => RedisCommands.hexists[RedisIO](key, it._1).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._
    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(it => c.hExists(key, it._1)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(it => hExists(key, it._1)))
}
