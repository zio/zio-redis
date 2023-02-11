package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultStreamBuilder
import zio.redis._
import zio.redis.options.PubSub.NumberOfSubscribers
import zio.schema.Schema
import zio.stream._
import zio.{Chunk, IO, Promise, Ref, ZIO}

trait PubSub extends RedisEnvironment {
  import PubSub._

  final def subscribe(channel: String): ResultStreamBuilder[Id] =
    subscribeWithCallback(channel)(emptyCallback)

  final def subscribeWithCallback(channel: String)(onSubscribe: PubSubCallback): ResultStreamBuilder[Id] =
    new ResultStreamBuilder[Id] {
      def returning[R: Schema]: IO[RedisError, Id[Stream[RedisError, R]]] =
        runSubscription(PubSubCommand.Subscribe(channel, List.empty), onSubscribe).flatMap(extractOne(channel, _))
    }

  final def subscribe(channel: String, channels: String*): ResultStreamBuilder[Chunk] =
    createStreamListBuilder(channel, channels.toList, emptyCallback, isPatterned = false)

  final def subscribeWithCallback(channel: String, channels: String*)(
    onSubscribe: PubSubCallback
  ): ResultStreamBuilder[Chunk] =
    createStreamListBuilder(channel, channels.toList, onSubscribe, isPatterned = false)

  final def pSubscribe(pattern: String): ResultStreamBuilder[Id] =
    pSubscribeWithCallback(pattern)(emptyCallback)

  final def pSubscribeWithCallback(
    pattern: String
  )(onSubscribe: PubSubCallback): ResultStreamBuilder[Id] =
    new ResultStreamBuilder[Id] {
      def returning[R: Schema]: IO[RedisError, Id[Stream[RedisError, R]]] =
        runSubscription(PubSubCommand.PSubscribe(pattern, List.empty), onSubscribe).flatMap(extractOne(pattern, _))
    }

  final def pSubscribe(pattern: String, patterns: String*): ResultStreamBuilder[Chunk] =
    createStreamListBuilder(pattern, patterns.toList, emptyCallback, isPatterned = true)

  final def pSubscribeWithCallback(pattern: String, patterns: String*)(
    onSubscribe: PubSubCallback
  ): ResultStreamBuilder[Chunk] =
    createStreamListBuilder(pattern, patterns.toList, onSubscribe, isPatterned = true)

  final def unsubscribe(channels: String*): IO[RedisError, Promise[RedisError, Chunk[(String, Long)]]] =
    runUnsubscription(PubSubCommand.Unsubscribe(channels.toList))

  final def pUnsubscribe(patterns: String*): IO[RedisError, Promise[RedisError, Chunk[(String, Long)]]] =
    runUnsubscription(PubSubCommand.PUnsubscribe(patterns.toList))

  final def publish[A: Schema](channel: String, message: A): IO[RedisError, Long] = {
    val command = RedisCommand(Publish, Tuple2(StringInput, ArbitraryInput[A]()), LongOutput, codec, executor)
    command.run((channel, message))
  }

  final def pubSubChannels(pattern: String): IO[RedisError, Chunk[String]] = {
    val command = RedisCommand(PubSubChannels, StringInput, ChunkOutput(MultiStringOutput), codec, executor)
    command.run(pattern)
  }

  final def pubSubNumPat: IO[RedisError, Long] = {
    val command = RedisCommand(PubSubNumPat, NoInput, LongOutput, codec, executor)
    command.run(())
  }

  final def pubSubNumSub(channel: String, channels: String*): IO[RedisError, Chunk[NumberOfSubscribers]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput, codec, executor)
    command.run((channel, channels.toList))
  }

  private def createStreamListBuilder(
    key: String,
    keys: List[String],
    callback: PubSubCallback,
    isPatterned: Boolean
  ): ResultStreamBuilder[Chunk] =
    new ResultStreamBuilder[Chunk] {
      def returning[R: Schema]: IO[RedisError, Chunk[Stream[RedisError, R]]] =
        if (isPatterned) runSubscription(PubSubCommand.PSubscribe(key, keys), callback)
        else runSubscription(PubSubCommand.Subscribe(key, keys), callback)
    }

  private def runUnsubscription(
    command: PubSubCommand
  ): IO[RedisError, Promise[RedisError, Chunk[(String, Long)]]] =
    for {
      promise <- Promise.make[RedisError, Chunk[(String, Long)]]
      ref     <- Ref.make(Chunk.empty[(String, Long)])
      streams <- RedisPubSubCommand(command, codec, pubSub).run[Unit] {
                   case PushProtocol.Unsubscribe(channel, numOfSubscription) =>
                     ref.update(_ appended (channel, numOfSubscription))
                   case PushProtocol.PUnsubscribe(pattern, numOfSubscription) =>
                     ref.update(_ appended (pattern, numOfSubscription))
                   case _ => ZIO.unit
                 }
      _ <- streams
             .fold(ZStream.empty)(_ merge _)
             .runDrain
             .onDone(
               e => promise.fail(e),
               _ => ref.get.flatMap(promise.succeed(_))
             )
             .fork
    } yield promise

  private def runSubscription[R: Schema](
    command: PubSubCommand,
    onSubscribe: PubSubCallback
  ): IO[RedisError, Chunk[Stream[RedisError, R]]] =
    RedisPubSubCommand(command, codec, pubSub).run[R] {
      case PushProtocol.Subscribe(key, numOfSubscription)  => onSubscribe(key, numOfSubscription)
      case PushProtocol.PSubscribe(key, numOfSubscription) => onSubscribe(key, numOfSubscription)
      case _                                               => ZIO.unit
    }

  private def extractOne[A](key: String, elements: Chunk[A]) =
    ZIO.fromOption(elements.headOption).orElseFail(RedisError.NoPubSubStream(key))
}

private[redis] object PubSub {
  private lazy val emptyCallback = (_: String, _: Long) => ZIO.unit

  final val Subscribe      = "SUBSCRIBE"
  final val Unsubscribe    = "UNSUBSCRIBE"
  final val PSubscribe     = "PSUBSCRIBE"
  final val PUnsubscribe   = "PUNSUBSCRIBE"
  final val Publish        = "PUBLISH"
  final val PubSubChannels = "PUBSUB CHANNELS"
  final val PubSubNumPat   = "PUBSUB NUMPAT"
  final val PubSubNumSub   = "PUBSUB NUMSUB"
}
