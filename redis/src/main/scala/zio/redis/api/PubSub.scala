package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultOutputStreamBuilder
import zio.redis._
import zio.redis.api.PubSub._
import zio.schema.Schema
import zio.stream._
import zio.{Chunk, ZIO}

trait PubSub {
  final def subscribeStreamBuilder(channel: String, channels: String*): ResultOutputStreamBuilder =
    new ResultOutputStreamBuilder {
      override def returning[R: Schema]: ZStream[Redis, RedisError, R] =
        ZStream.serviceWithStream[Redis] { redis =>
          RedisPubSubCommand
            .run(RedisPubSubCommand.Subscribe(channel, channels.toList))
            .collect { case t: PushProtocol.Message => t.message }
            .mapZIO(resp =>
              ZIO
                .attempt(ArbitraryOutput[R]().unsafeDecode(resp)(redis.codec))
                .refineToOrDie[RedisError]
            )
        }
    }

  final def subscribe(channel: String, channels: String*): ZStream[Redis, RedisError, PushProtocol] =
    RedisPubSubCommand.run(RedisPubSubCommand.Subscribe(channel, channels.toList))

  final def unsubscribe(channels: String*): ZStream[Redis, RedisError, PushProtocol.Unsubscribe] =
    RedisPubSubCommand.run(RedisPubSubCommand.Unsubscribe(channels.toList)).collect {
      case t: PushProtocol.Unsubscribe => t
    }

  final def pSubscribe(pattern: String, patterns: String*): ZStream[Redis, RedisError, PushProtocol] =
    RedisPubSubCommand.run(RedisPubSubCommand.PSubscribe(pattern, patterns.toList))

  final def pSubscribeStreamBuilder(pattern: String, patterns: String*): ResultOutputStreamBuilder =
    new ResultOutputStreamBuilder {
      override def returning[R: Schema]: ZStream[Redis, RedisError, R] =
        ZStream.serviceWithStream[Redis] { redis =>
          RedisPubSubCommand
            .run(RedisPubSubCommand.PSubscribe(pattern, patterns.toList))
            .collect { case t: PushProtocol.PMessage => t.message }
            .mapZIO(resp =>
              ZIO
                .attempt(ArbitraryOutput[R]().unsafeDecode(resp)(redis.codec))
                .refineToOrDie[RedisError]
            )
        }
    }

  final def pUnsubscribe(patterns: String*): ZStream[Redis, RedisError, PushProtocol.PUnsubscribe] =
    RedisPubSubCommand.run(RedisPubSubCommand.PUnsubscribe(patterns.toList)).collect {
      case t: PushProtocol.PUnsubscribe => t
    }

  final def publish[A: Schema](channel: String, message: A): ZIO[Redis, RedisError, Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryInput[A]()), LongOutput)
    command.run((channel, message))
  }

  final def pubSubChannels(pattern: String): ZIO[Redis, RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, StringInput, ChunkOutput(MultiStringOutput))
    command.run(pattern)
  }

  final def pubSubNumPat: ZIO[Redis, RedisError, Long] = {
    val command = RedisCommand(PubSubNumPat, NoInput, LongOutput)
    command.run(())
  }

  final def pubSubNumSub(channel: String, channels: String*): ZIO[Redis, RedisError, Chunk[NumSubResponse]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput)
    command.run((channel, channels.toList))
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
