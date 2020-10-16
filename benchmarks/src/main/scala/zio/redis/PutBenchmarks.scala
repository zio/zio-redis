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
class PutBenchmarks {
  import BenchmarkRuntime._

  @Param(Array("500"))
  private var count: Int = _

  private var items: List[String] = _

  @Setup(Level.Trial)
  def setup(): Unit =
    items = (0 to count).toList.map(_.toString)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc._
    import _root_.laserdisc.auto._
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{ all => cmd }
    import cats.instances.list._
    import cats.syntax.foldable._

    RedisClient
      .to(RedisHost, RedisPort)
      .use(c => items.traverse_(i => c.send(cmd.set(Key.unsafeFrom(i), i))))
      .unsafeRunSync
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.effect._
    import cats.implicits._
    import fs2.io.tcp._
    import io.chrisdavenport.rediculous._

    val connection =
      for {
        blocker <- Blocker[IO]
        sg      <- SocketGroup[IO](blocker)
        c       <- RedisConnection.queued[IO](sg, RedisHost, RedisPort, maxQueued = 10000, workers = 2)
      } yield c

    connection
      .use(c => items.traverse_(i => RedisCommands.set[RedisIO](i, i).run(c)))
      .unsafeRunSync
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.effect.IO
    import cats.instances.list._
    import cats.syntax.foldable._
    import dev.profunktor.redis4cats.Redis
    import dev.profunktor.redis4cats.effect.Log.NoOp._

    Redis[IO]
      .utf8(s"redis://$RedisHost:$RedisPort")
      .use(c => items.traverse_(i => c.set(i, i)))
      .unsafeRunSync
  }

  @Benchmark
  def zio(): Unit = {
    val effect = ZIO
      .foreach_(items)(i => set(i, i, None, None, None))
      .provideLayer(RedisExecutor.liveServer(RedisHost, RedisPort).orDie)

    unsafeRun(effect)
  }
}
