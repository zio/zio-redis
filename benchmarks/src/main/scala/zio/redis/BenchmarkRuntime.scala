package zio.redis

import zio.BootstrapRuntime
import zio.internal.Platform

trait BenchmarkRuntime extends BootstrapRuntime with RedisClients with BenchmarksUtils {
  override val platform: Platform = Platform.benchmark
}
