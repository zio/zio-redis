package zio.redis

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Warmup(iterations = 15, timeUnit = TimeUnit.SECONDS, time = 3)
@Fork(3)
final class PutBenchmark {
  import BenchmarkRuntime._

  @Param(Array("500"))
  private var count: Int = _

  private val items: List[Int] = (0 to count).toList

  @Benchmark
  def laserdisc(): Unit = ???

  @Benchmark
  def redis4cats(): Unit = {
    import cats.effect.IO
    import cats.implicits._
    import dev.profunktor.redis4cats.Redis
    import dev.profunktor.redis4cats.effect.Log.Stdout._

    Redis[IO]
      .utf8(Host)
      .use(cmd => items.traverse(i => cmd.set(s"redis4cats-$i", s"redis4cats-$i")))
      .unsafeRunSync
  }

  @Benchmark
  def zio(): Unit = ???
}
