package zio.redis

import cats.effect.{ IO => CatsIO }

import zio.logging.Logging
import zio.{ BootstrapRuntime, ZIO, ZLayer }

trait BenchmarksUtils {
  self: RedisClients with BootstrapRuntime =>

  def unsafeRun[CL](f: CL => CatsIO[Unit])(implicit unsafeRunner: QueryUnsafeRunner[CL]): Unit =
    unsafeRunner.unsafeRun(f)

  def zioUnsafeRun(source: ZIO[RedisExecutor, RedisError, Unit]): Unit =
    unsafeRun(source.provideLayer(BenchmarksUtils.Layer))
}

object BenchmarksUtils {
  final val RedisHost = "127.0.0.1"
  final val RedisPort = 6379

  private final val Layer =
    Logging.ignore ++ ZLayer.succeed(RedisConfig(RedisHost, RedisPort)) >>> RedisExecutor.live.orDie
}
