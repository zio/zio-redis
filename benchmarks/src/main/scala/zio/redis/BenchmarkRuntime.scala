package zio.redis

import cats.effect.{ ContextShift, IO => CatsIO }
import zio.BootstrapRuntime
import zio.internal.Platform

import scala.concurrent.ExecutionContext

object BenchmarkRuntime extends BootstrapRuntime {
  implicit val cs: ContextShift[CatsIO] = CatsIO.contextShift(ExecutionContext.global)

  override val platform: Platform = Platform.benchmark

  final val Host = "127.0.0.1"
  final val Port = 6379
}
