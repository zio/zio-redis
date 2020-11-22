package zio.redis

import zio.BootstrapRuntime
import zio.internal.Platform

object BenchmarkRuntime extends BootstrapRuntime with RedisClients with BenchmarksUtils with EffectContexts {
  override val platform: Platform = Platform.benchmark
}
