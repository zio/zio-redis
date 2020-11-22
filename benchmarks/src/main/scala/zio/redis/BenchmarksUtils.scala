package zio.redis

import cats.effect.{IO => CatsIO}
import zio.{BootstrapRuntime, ZIO}
import zio.logging.Logging

trait BenchmarksUtils {
  self: RedisClients with BootstrapRuntime =>

  def unsafeRun[CL](f: CL => CatsIO[Unit])(implicit unsafeRunner: QueryUnsafeRunner[CL]): Unit =
    unsafeRunner.unsafeRun(f)

  def zioUnsafeRun(source: ZIO[RedisExecutor, RedisError, Unit]): Unit =
    unsafeRun(source.provideLayer(Logging.ignore >>> RedisExecutor.live(RedisHost, RedisPort).orDie))
}
