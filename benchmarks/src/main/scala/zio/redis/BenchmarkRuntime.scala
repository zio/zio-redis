package zio.redis

import scala.concurrent.ExecutionContext
import cats.effect.{ ContextShift, Timer, IO => CatsIO }
import zio.BootstrapRuntime
import zio.internal.Platform

object BenchmarkRuntime extends BootstrapRuntime {
  implicit val cs: ContextShift[CatsIO] = CatsIO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[CatsIO]     = CatsIO.timer(ExecutionContext.global)

  override val platform: Platform = Platform.benchmark

  final val RedisHost = "127.0.0.1"
  final val RedisPort = 6379
}
