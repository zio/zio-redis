package zio.redis.api

import zio.redis.RedisCommand
import zio.redis.Input._
import zio.redis.Output._

trait Connection {
  final val auth   = RedisCommand("AUTH", StringInput, UnitOutput)
  final val echo   = RedisCommand("ECHO", StringInput, StringOutput)
  final val ping   = RedisCommand("PING", Varargs(StringInput), StringOutput)
  final val select = RedisCommand("SELECT", LongInput, UnitOutput)
}
