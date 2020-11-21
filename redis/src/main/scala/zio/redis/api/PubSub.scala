package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait PubSub {
  import PubSub._

  final def pSubscribe(pattern: String, patterns: String*): ZIO[RedisExecutor, RedisError, Unit] =
    PSubscribe.run((pattern, patterns.toList))

  final def pubSubChannels(pattern: String): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    PubSubChannels.run(("CHANNELS", List(pattern)))

  final def pubSubNumSub(channel: String, channels: String*): ZIO[RedisExecutor, RedisError, Map[String, Long]] =
    PubSubNumSub.run(("NUMSUB", channel :: channels.toList))

  final def pubSubNumPat(): ZIO[RedisExecutor, RedisError, Long] = PubSubNumPat.run("NUMPAT")

  final def publish(channel: String, msg: String): ZIO[RedisExecutor, RedisError, Long] = Publish.run((channel, msg))

  final def pUnsubscribe(patterns: String*): ZIO[RedisExecutor, RedisError, Unit] = PUnsubscribe.run(patterns.toList)

  final def subscribe(channel: String, channels: String*): ZIO[RedisExecutor, RedisError, Unit] =
    Subscribe.run((channel, channels.toList))

  final def unsubscribe(channels: String*): ZIO[RedisExecutor, RedisError, Unit] = Unsubscribe.run(channels.toList)

}

private object PubSub {
  final val PSubscribe     = RedisCommand("PSUBSCRIBE", NonEmptyList(StringInput), UnitOutput)
  final val PubSubChannels = RedisCommand("PUBSUB", NonEmptyList(StringInput), ChunkOutput)
  final val PubSubNumSub   = RedisCommand("PUBSUB", NonEmptyList(StringInput), KeyLongValueOutput)
  final val PubSubNumPat   = RedisCommand("PUBSUB", StringInput, LongOutput)
  final val Publish        = RedisCommand("PUBLISH", Tuple2(StringInput, StringInput), LongOutput)
  final val PUnsubscribe   = RedisCommand("PUNSUBSCRIBE", ListInput(StringInput), UnitOutput)
  final val Subscribe      = RedisCommand("SUBSCRIBE", NonEmptyList(StringInput), UnitOutput)
  final val Unsubscribe    = RedisCommand("UNSUBSCRIBE", ListInput(StringInput), UnitOutput)

}
