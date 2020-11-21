package zio.redis

import cats.effect.{ IO => CatsIO }
import zio.ZIO
import zio.logging.Logging
import zio.redis.RedisClients.QueryUnsafeRunner

object BenchmarksUtils {

  import BenchmarkRuntime.RedisHost
  import BenchmarkRuntime.RedisPort
  import BenchmarkRuntime.unsafeRun

  def unsafeClientRun[CL](f: CL => CatsIO[Unit])(implicit unsafeRunner: QueryUnsafeRunner[CL]): Unit =
    unsafeRunner.unsafeRun(f)

  def zioUnsafeRun(source: ZIO[RedisExecutor, RedisError, Unit]): Unit =
    unsafeRun(source.provideLayer(Logging.ignore >>> RedisExecutor.live(RedisHost, RedisPort).orDie))
}
