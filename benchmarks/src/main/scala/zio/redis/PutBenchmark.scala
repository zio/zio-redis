package zio.redis

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import zio.ZIO

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(3)
class PutBenchmark {
  import BenchmarkRuntime._

  @Param(Array("500"))
  private var count: Int = _

  private val items: List[Int] = (0 to count).toList

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc._
    import _root_.laserdisc.{ all => cmd }
    import _root_.laserdisc.auto._
    import _root_.laserdisc.fs2._
    import cats.instances.list._
    import cats.syntax.foldable._

    RedisClient
      .to(RedisHost, RedisPort)
      .use(c => items.traverse_(i => c.send(cmd.set(Key.unsafeFrom(s"$i"), i))))
      .unsafeRunSync
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.effect.IO
    import cats.instances.list._
    import cats.syntax.foldable._
    import dev.profunktor.redis4cats.Redis
    import dev.profunktor.redis4cats.effect.Log.Stdout._

    Redis[IO]
      .utf8(RedisHost)
      .use(c => items.traverse_(i => c.set(s"$i", s"$i")))
      .unsafeRunSync
  }

  @Benchmark
  def zio(): Unit = {
    val effect = ZIO
      .foreach_(items)(i => set(s"$i", s"$i", None, None, None))
      .provideLayer(RedisExecutor.live(RedisHost, RedisPort).orDie)

    unsafeRun(effect)
  }
}
