package zio.redis.api

import zio.redis.ConnectionType._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.RedisCommand

trait Streams {
  final val xAck =
    RedisCommand("XACK", Tuple3(StringInput, StringInput, NonEmptyList(StringInput)), LongOutput, Streams)
}
