package zio.redis.internal

import zio.Scope

final case class ExecutorScope(executor: RedisExecutor, scope: Scope.Closeable)
