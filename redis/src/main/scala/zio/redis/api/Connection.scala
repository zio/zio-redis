package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.redis.Key.NoKey

trait Connection {
  import Connection._

  final def auth(a: String): ZIO[RedisExecutor, RedisError, Unit] = Auth.run(a)

  final def echo(a: String): ZIO[RedisExecutor, RedisError, String] = Echo.run(a)

  final def ping(as: String*): ZIO[RedisExecutor, RedisError, String] = Ping.run(as)

  final def select(a: Long): ZIO[RedisExecutor, RedisError, Unit] = Select.run(a)
}

private object Connection {
  final val Auth   = RedisCommand("AUTH", StringInput, UnitOutput, NoKey)
  final val Echo   = RedisCommand("ECHO", StringInput, MultiStringOutput, NoKey)
  final val Ping   = RedisCommand("PING", Varargs(StringInput), MultiStringOutput, NoKey)
  final val Select = RedisCommand("SELECT", LongInput, UnitOutput, NoKey)
}
