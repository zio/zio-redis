package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.redis.api.PubSub._
import zio.schema.Schema
import zio.stream.ZStream
import zio.{Chunk, ZIO}

trait PubSub {
  final def subscribe(channel: String, channels: String*): ZStream[Redis, RedisError, PushProtocol] = {
    val in = (channel, channels.toList)
    RedisPubSubCommand(Subscribe, NonEmptyList(StringInput)).run(in)
  }

  final def unsubscribe(channels: String*): ZStream[Redis, RedisError, PushProtocol] =
    RedisPubSubCommand(Unsubscribe, Varargs(StringInput)).run(channels.toList)

  final def pSubscribe(pattern: String, patterns: String*): ZStream[Redis, RedisError, PushProtocol] = {
    val in = (Pattern(pattern), patterns.toList.map(Pattern(_)))
    RedisPubSubCommand(PSubscribe, NonEmptyList(PatternInput)).run(in)
  }

  final def pUnsubscribe(patterns: String*): ZStream[Redis, RedisError, PushProtocol] =
    RedisPubSubCommand(PUnsubscribe, Varargs(PatternInput)).run(patterns.toList.map(Pattern(_)))

  final def publish[A: Schema](channel: String, message: A): ZIO[Redis, RedisError, Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryInput[A]), LongOutput)
    command.run(channel, message)
  }

  final def pubSubChannels(pattern: String): ZIO[Redis, RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, PatternInput, ChunkOutput(StringOutput))
    command.run(Pattern(pattern))
  }

  final def pubSubNumPat: ZIO[Redis, RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubNumPat, NoInput, ChunkOutput(StringOutput))
    command.run()
  }

  final def pubSubNumSub(channel: String, channels: String*): ZIO[Redis, RedisError, Chunk[NumSubResponse]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput)
    command.run(channel, channels.toList)
  }
}

private[redis] object PubSub {
  final val Subscribe      = "SUBSCRIBE"
  final val Unsubscribe    = "UNSUBSCRIBE"
  final val PSubscribe     = "PSUBSCRIBE"
  final val PUnsubscribe   = "PUNSUBSCRIBE"
  final val Publish        = "PUBLISH"
  final val PubSubChannels = "PUBSUB CHANNELS"
  final val PubSubNumPat   = "PUBSUB NUMPAT"
  final val PubSubNumSub   = "PUBSUB NUMSUB"
}
