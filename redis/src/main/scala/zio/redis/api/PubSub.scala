package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.stream.ZStream
import zio.{ Chunk, ZIO }

trait PubSub {
  import PubSub._

  final def pSubscribe(pattern: String, patterns: String*): ZStream[RedisExecutor, RedisError, String] =
    PSubscribe.runStream((pattern, patterns.toList))

  final def pubSubChannels(pattern: String): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    PubSubChannels.run(("CHANNELS", List(pattern)))

  final def pubSubNumSub(channel: String, channels: String*): ZIO[RedisExecutor, RedisError, Map[String, Long]] =
    PubSubNumSub.run(("NUMSUB", channel :: channels.toList))

  final def pubSubNumPat(): ZIO[RedisExecutor, RedisError, Long] = PubSubNumPat.run("NUMPAT")

  final def publish(channel: String, msg: String): ZIO[RedisExecutor, RedisError, Long] = Publish.run((channel, msg))

  final def pUnsubscribe(patterns: String*): ZIO[RedisExecutor, RedisError, Unit] = PUnsubscribe.run(patterns.toList)

  final def subscribe(channel: String, channels: String*): ZStream[RedisExecutor, RedisError, String] =
    Subscribe.runStream((channel, channels.toList))

  final def unsubscribe(channels: String*): ZIO[RedisExecutor, RedisError, Unit] = Unsubscribe.run(channels.toList)

}

private object PubSub {
  final val PSubscribe     = RedisCommand("PSUBSCRIBE", NonEmptyList(StringInput), StringOutput)
  final val PubSubChannels = RedisCommand("PUBSUB", NonEmptyList(StringInput), ChunkOutput)
  final val PubSubNumSub   = RedisCommand("PUBSUB", NonEmptyList(StringInput), KeyLongValueOutput)
  final val PubSubNumPat   = RedisCommand("PUBSUB", StringInput, LongOutput)
  final val Publish        = RedisCommand("PUBLISH", Tuple2(StringInput, StringInput), LongOutput)
  final val PUnsubscribe   = RedisCommand("PUNSUBSCRIBE", ListInput(StringInput), UnitOutput)
  final val Subscribe      = RedisCommand("SUBSCRIBE", NonEmptyList(StringInput), StringOutput)
  final val Unsubscribe    = RedisCommand("UNSUBSCRIBE", ListInput(StringInput), UnitOutput)

}
