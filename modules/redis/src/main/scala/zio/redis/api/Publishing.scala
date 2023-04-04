package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.redis.internal.{RedisCommand, RedisEnvironment}
import zio.redis.options.PubSub.NumberOfSubscribers
import zio.schema.Schema
import zio.{Chunk, IO}

trait Publishing extends RedisEnvironment {
  import Publishing._

  final def publish[A: Schema](channel: String, message: A): IO[RedisError, Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryKeyInput[A]()), LongOutput, executor)
    command.run((channel, message))
  }

  final def pubSubChannels(pattern: String): IO[RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, StringInput, ChunkOutput(MultiStringOutput), executor)
    command.run(pattern)
  }

  final def pubSubNumPat: IO[RedisError, Long] = {
    val command = RedisCommand(PubSubNumPat, NoInput, LongOutput, executor)
    command.run(())
  }

  final def pubSubNumSub(channel: String, channels: String*): IO[RedisError, Chunk[NumberOfSubscribers]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput, executor)
    command.run((channel, channels.toList))
  }
}

private[redis] object Publishing {
  final val Publish        = "PUBLISH"
  final val PubSubChannels = "PUBSUB CHANNELS"
  final val PubSubNumPat   = "PUBSUB NUMPAT"
  final val PubSubNumSub   = "PUBSUB NUMSUB"
}
