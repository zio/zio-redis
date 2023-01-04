package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultOutputStreamBuilder
import zio.redis._
import zio.redis.api.PubSub._
import zio.schema.Schema
import zio.stream.ZStream
import zio.{Chunk, ZIO}

trait PubSub {
  final def subscribe(channel: String, channels: String*): ResultOutputStreamBuilder = new ResultOutputStreamBuilder {
    override def returning[R: Schema]: ZStream[Redis, RedisError, PushProtocol[R]] = {
      val in   = (channel, channels.toList)
      val keys = channels.appended(channel).toSet
      RedisPubSubCommand(Subscribe, NonEmptyList(StringInput), ArbitraryOutput[R]())
        .run(in)
        .filter(keys contains _.key)
        .collect {
          case t: PushProtocol.Subscribe   => t
          case t: PushProtocol.Unsubscribe => t
          case t: PushProtocol.Message[_]  => t
        }
    }
  }

  final def unsubscribe(channels: String*): ZStream[Redis, RedisError, PushProtocol.Unsubscribe] = {
    val keys = channels.toSet
    RedisPubSubCommand(Unsubscribe, Varargs(StringInput), BulkStringOutput)
      .run(channels.toList)
      .collect { case t: PushProtocol.Unsubscribe => t }
      .filter(msg =>
        if (keys.isEmpty) true
        else keys contains msg.key
      )
  }

  final def pSubscribe(pattern: String, patterns: String*): ResultOutputStreamBuilder = new ResultOutputStreamBuilder {
    override def returning[R: Schema]: ZStream[Redis, RedisError, PushProtocol[R]] = {
      val in   = (pattern, patterns.toList)
      val keys = patterns.appended(pattern).toSet
      RedisPubSubCommand(PSubscribe, NonEmptyList(StringInput), ArbitraryOutput[R]())
        .run(in)
        .filter(keys contains _.key)
        .collect {
          case t: PushProtocol.PSubscribe   => t
          case t: PushProtocol.PUnsubscribe => t
          case t: PushProtocol.PMessage[_]  => t
        }
    }
  }

  final def pUnsubscribe(patterns: String*): ZStream[Redis, RedisError, PushProtocol.PUnsubscribe] = {
    val keys = patterns.toSet
    RedisPubSubCommand(PUnsubscribe, Varargs(StringInput), BulkStringOutput)
      .run(patterns.toList)
      .collect { case t: PushProtocol.PUnsubscribe => t }
      .filter(msg =>
        if (keys.isEmpty) true
        else keys contains msg.key
      )
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
