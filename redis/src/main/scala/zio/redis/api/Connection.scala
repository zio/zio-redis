package zio.redis.api

import zio.ZIO
import zio.redis.Input._
import zio.redis.Output._
import zio.redis._

trait Connection {
  import Connection._

  final def auth(a: String): ZIO[RedisExecutor, RedisError, Unit] = Auth.run(a)

  final def echo(a: String): ZIO[RedisExecutor, RedisError, String] = Echo.run(a)

  final def ping(a: Option[String] = None): ZIO[RedisExecutor, RedisError, String] = Ping.run(a)

  final def select(a: Long): ZIO[RedisExecutor, RedisError, Unit] = Select.run(a)
}

private[redis] object Connection {
  final val Auth   = RedisCommand("AUTH", StringInput, UnitOutput)
  final val Echo   = RedisCommand("ECHO", StringInput, MultiStringOutput)
  final val Ping   = RedisCommand("PING", OptionalInput(StringInput), SingleOrMultiStringOutput)
  final val Select = RedisCommand("SELECT", LongInput, UnitOutput)
}
