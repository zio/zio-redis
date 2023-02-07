package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.ResultBuilder.ResultStreamBuilder
import zio.redis._
import zio.schema.Schema
import zio.stream._
import zio.{Chunk, IO, Promise, ZIO}

trait PubSub extends RedisEnvironment {
  import PubSub._

  final def subscribe(channel: String): ResultStreamBuilder[Id] =
    subscribeWithCallback(channel)(emptyCallback)

  final def subscribe(channel: String, channels: List[String]): ResultStreamBuilder[List] =
    subscribeWithCallback(channel, channels)(emptyCallback)

  final def subscribeWithCallback(channel: String)(onSubscribe: PubSubCallback): ResultStreamBuilder[Id] =
    new ResultStreamBuilder[Id] {
      def returning[R: Schema]: IO[RedisError, Id[Stream[RedisError, R]]] =
        getSubscribeStreams(channel, List.empty)(onSubscribe).flatMap(extractOne(channel, _))
    }

  final def subscribeWithCallback(
    channel: String,
    channels: List[String]
  )(onSubscribe: PubSubCallback): ResultStreamBuilder[List] =
    new ResultStreamBuilder[List] {
      def returning[R: Schema]: IO[RedisError, List[Stream[RedisError, R]]] =
        getSubscribeStreams(channel, channels)(onSubscribe)
    }

  final def unsubscribe(channel: String): IO[RedisError, Promise[RedisError, Unit]] =
    unsubscribeWithCallback(List(channel))(emptyCallback).flatMap(extractOne(channel, _))

  final def unsubscribe(channels: List[String]): IO[RedisError, List[Promise[RedisError, Unit]]] =
    unsubscribeWithCallback(channels)(emptyCallback)

  final def unsubscribeWithCallback(channel: String)(
    onUnsubscribe: PubSubCallback
  ): IO[RedisError, Promise[RedisError, Unit]] =
    unsubscribeWithCallback(List(channel))(onUnsubscribe).flatMap(extractOne(channel, _))

  final def unsubscribeWithCallback(channels: List[String])(
    onUnsubscribe: PubSubCallback
  ): IO[RedisError, List[Promise[RedisError, Unit]]] =
    RedisPubSubCommand(PubSubCommand.Unsubscribe(channels), codec, pubSub).run
      .flatMap(streams =>
        ZIO.foreach(streams)(stream =>
          for {
            promise <- Promise.make[RedisError, Unit]
            _ <- stream
                   .interruptWhen(promise)
                   .mapZIO {
                     case PushProtocol.Unsubscribe(key, numOfSubscription) =>
                       onUnsubscribe(key, numOfSubscription) <* promise.succeed(())
                     case _ => ZIO.unit
                   }
                   .runDrain
                   .fork
          } yield promise
        )
      )

  private def getSubscribeStreams[R: Schema](channel: String, channels: List[String])(onSubscribe: PubSubCallback) =
    RedisPubSubCommand(PubSubCommand.Subscribe(channel, channels), codec, pubSub).run
      .flatMap(streams =>
        ZIO.foreach(streams)(stream =>
          Promise
            .make[RedisError, Unit]
            .map(promise =>
              stream
                .interruptWhen(promise)
                .mapZIO {
                  case PushProtocol.Subscribe(key, numOfSubscription) =>
                    onSubscribe(key, numOfSubscription).as(None)
                  case _: PushProtocol.Unsubscribe =>
                    promise.succeed(()).as(None)
                  case PushProtocol.Message(_, msg) =>
                    ZIO
                      .attempt(ArbitraryOutput[R]().unsafeDecode(msg)(codec))
                      .refineToOrDie[RedisError]
                      .asSome
                  case _ => ZIO.none
                }
                .collectSome
            )
        )
      )

  final def pSubscribe(pattern: String): ResultStreamBuilder[Id] = pSubscribeWithCallback(pattern)(emptyCallback)

  final def pSubscribeWithCallback(pattern: String)(onSubscribe: PubSubCallback): ResultStreamBuilder[Id] =
    new ResultStreamBuilder[Id] {
      def returning[R: Schema]: IO[RedisError, Id[Stream[RedisError, R]]] =
        getPSubscribeStreams(pattern, List.empty)(onSubscribe).flatMap(extractOne(pattern, _))
    }

  final def pSubscribe(pattern: String, patterns: List[String]): ResultStreamBuilder[List] =
    pSubscribeWithCallback(pattern, patterns)(emptyCallback)

  final def pSubscribeWithCallback(pattern: String, patterns: List[String])(
    onSubscribe: PubSubCallback
  ): ResultStreamBuilder[List] =
    new ResultStreamBuilder[List] {
      def returning[R: Schema]: IO[RedisError, List[Stream[RedisError, R]]] =
        getPSubscribeStreams(pattern, patterns)(onSubscribe)
    }

  private def getPSubscribeStreams[R: Schema](pattern: String, patterns: List[String])(onSubscribe: PubSubCallback) =
    RedisPubSubCommand(PubSubCommand.PSubscribe(pattern, patterns), codec, pubSub).run
      .flatMap(streams =>
        ZIO.foreach(streams)(stream =>
          Promise
            .make[RedisError, Unit]
            .map(promise =>
              stream
                .interruptWhen(promise)
                .mapZIO {
                  case PushProtocol.PSubscribe(key, numOfSubscription) =>
                    onSubscribe(key, numOfSubscription).as(None)
                  case _: PushProtocol.PUnsubscribe =>
                    promise.succeed(()).as(None)
                  case PushProtocol.PMessage(_, _, msg) =>
                    ZIO
                      .attempt(ArbitraryOutput[R]().unsafeDecode(msg)(codec))
                      .refineToOrDie[RedisError]
                      .asSome
                  case _ => ZIO.none
                }
                .collectSome
            )
        )
      )

  final def pUnsubscribe(pattern: String): IO[RedisError, Promise[RedisError, Unit]] =
    pUnsubscribeWithCallback(List(pattern))(emptyCallback).flatMap(extractOne(pattern, _))

  final def pUnsubscribeWithCallback(pattern: String)(
    onUnsubscribe: PubSubCallback
  ): IO[RedisError, Promise[RedisError, Unit]] =
    pUnsubscribeWithCallback(List(pattern))(onUnsubscribe).flatMap(extractOne(pattern, _))

  final def pUnsubscribeWithCallback(
    patterns: List[String]
  )(onUnsubscribe: PubSubCallback): IO[RedisError, List[Promise[RedisError, Unit]]] =
    RedisPubSubCommand(PubSubCommand.PUnsubscribe(patterns), codec, pubSub).run
      .flatMap(streams =>
        ZIO.foreach(streams)(stream =>
          for {
            promise <- Promise.make[RedisError, Unit]
            _ <- stream
                   .interruptWhen(promise)
                   .mapZIO {
                     case PushProtocol.PUnsubscribe(key, numOfSubscription) =>
                       onUnsubscribe(key, numOfSubscription) <* promise.succeed(())
                     case _ => ZIO.unit
                   }
                   .runDrain
                   .fork
          } yield promise
        )
      )

  private def extractOne[A](key: String, elements: List[A]) =
    ZIO.fromOption(elements.headOption).orElseFail(RedisError.NoPubSubStream(key))

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

  final def pubSubNumSub(channel: String, channels: String*): IO[RedisError, Chunk[NumSubResponse]] = {
    val command = RedisCommand(PubSubNumSub, NonEmptyList(StringInput), NumSubResponseOutput, codec, executor)
    command.run((channel, channels.toList))
  }
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
