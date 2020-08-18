package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Connection {
  final val auth   = RedisCommand("AUTH", StringInput, UnitOutput, Base)
  final val echo   = RedisCommand("ECHO", StringInput, MultiStringOutput, Base)
  final val ping   = RedisCommand("PING", Varargs(StringInput), MultiStringOutput, Base)
  final val select = RedisCommand("SELECT", LongInput, UnitOutput, Base)
}
