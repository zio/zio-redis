package zio.redis.api

import zio.redis.ResultBuilder.ResultBuilder1
import zio.redis._
import zio.redis.options.PubSub.PubSubCallback
import zio.schema.Schema
import zio.stream.Stream
import zio.{Chunk, IO, ZIO}

trait Subscribe extends SubscribeEnvironment {
  import Subscribe._

  final def subscribe(channel: String, channels: String*) =
    new ResultBuilder1[({ type lambda[x] = Stream[RedisError, (String, x)] })#lambda] {
      def returning[R: Schema]: IO[RedisError, Stream[RedisError, (String, R)]] =
        RedisPubSubCommand(codec, executor).subscribe(
          Chunk.single(channel) ++ Chunk.fromIterable(channels),
          emptyCallback,
          emptyCallback
        )
    }

  final def subscribeWithCallback(channel: String, channels: String*)(
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ) =
    new ResultBuilder1[({ type lambda[x] = Stream[RedisError, (String, x)] })#lambda] {
      def returning[R: Schema]: IO[RedisError, Stream[RedisError, (String, R)]] =
        RedisPubSubCommand(codec, executor).subscribe(
          Chunk.single(channel) ++ Chunk.fromIterable(channels),
          onSubscribe,
          onUnsubscribe
        )
    }

  final def pSubscribe(pattern: String, patterns: String*) =
    new ResultBuilder1[({ type lambda[x] = Stream[RedisError, (String, x)] })#lambda] {
      def returning[R: Schema]: IO[RedisError, Stream[RedisError, (String, R)]] =
        RedisPubSubCommand(codec, executor).pSubscribe(
          Chunk.single(pattern) ++ Chunk.fromIterable(patterns),
          emptyCallback,
          emptyCallback
        )
    }

  final def pSubscribeWithCallback(
    pattern: String,
    patterns: String*
  )(onSubscribe: PubSubCallback, onUnsubscribe: PubSubCallback) =
    new ResultBuilder1[({ type lambda[x] = Stream[RedisError, (String, x)] })#lambda] {
      def returning[R: Schema]: IO[RedisError, Stream[RedisError, (String, R)]] =
        RedisPubSubCommand(codec, executor).pSubscribe(
          Chunk.single(pattern) ++ Chunk.fromIterable(patterns),
          onSubscribe,
          onUnsubscribe
        )
    }

  final def unsubscribe(channels: String*): IO[RedisError, Unit] =
    RedisPubSubCommand(codec, executor).unsubscribe(Chunk.fromIterable(channels))

  final def pUnsubscribe(patterns: String*): IO[RedisError, Unit] =
    RedisPubSubCommand(codec, executor).unsubscribe(Chunk.fromIterable(patterns))

}

object Subscribe {
  private lazy val emptyCallback = (_: String, _: Long) => ZIO.unit

  final val Subscribe    = "SUBSCRIBE"
  final val Unsubscribe  = "UNSUBSCRIBE"
  final val PSubscribe   = "PSUBSCRIBE"
  final val PUnsubscribe = "PUNSUBSCRIBE"
}
