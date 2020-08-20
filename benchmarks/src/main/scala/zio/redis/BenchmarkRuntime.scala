package zio.redis

import scala.concurrent.ExecutionContext

import cats.effect.{ ContextShift, Timer, IO => CatsIO }
import io.chrisdavenport.rediculous.Redis

import zio.BootstrapRuntime
import zio.internal.Platform

object BenchmarkRuntime extends BootstrapRuntime {
  implicit val cs: ContextShift[CatsIO] = CatsIO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[CatsIO]     = CatsIO.timer(ExecutionContext.global)

  override val platform: Platform = Platform.benchmark

  type RedisIO[A] = Redis[CatsIO, A]

  final val RedisHost = "127.0.0.1"
  final val RedisPort = 6379
}
