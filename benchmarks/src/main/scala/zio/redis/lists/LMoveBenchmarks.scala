package zio.redis.lists

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.redis._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class LMoveBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key = "test-list"

  @Setup(Level.Trial)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(rPush(key, items.head, items.tail: _*).unit)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    zioUnsafeRun(del(key).unit)

  @Benchmark
  def zio(): Unit = zioUnsafeRun(
    ZIO.foreach_(items)(_ => lMove[String, String, String](key, key, Side.Left, Side.Right))
  )
}
