package zio.redis

import zio.redis.Input._
import zio.redis.Output.{ArbitraryOutput, PushProtocolOutput}
import zio.redis.api.Subscription
import zio.redis.options.PubSub.PubSubCallback
import zio.schema.codec.BinaryCodec
import zio.stream.ZStream.RefineToOrDieOps
import zio.stream._
import zio.{Chunk, IO, Promise, Ref, ZIO}

private[redis] final case class RedisSubscriptionCommand(executor: SubscriptionExecutor) extends {
  import zio.redis.options.PubSub.PushProtocol._

  def subscribe[A: BinaryCodec](
    channels: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit codec: BinaryCodec[String]): IO[RedisError, Stream[RedisError, (String, A)]] =
    for {
      unsubscribedRef <- Ref.make(channels.map(_ -> false).toMap)
      promise         <- Promise.make[RedisError, Unit]
      channelSet       = channels.toSet
      stream <- executor
                  .execute(makeCommand(Subscription.Subscribe, channels))
                  .map(
                    _.mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp))).mapZIO {
                      case Subscribe(channel, numOfSubscription) if channelSet contains channel =>
                        onSubscribe(channel, numOfSubscription).as(None)
                      case Message(channel, message) if channelSet contains channel =>
                        ZIO
                          .attempt(ArbitraryOutput[A]().unsafeDecode(message))
                          .map(msg => Some((channel, msg)))
                      case Unsubscribe(channel, numOfSubscription) if channelSet contains channel =>
                        for {
                          _ <- onUnsubscribe(channel, numOfSubscription)
                          _ <- unsubscribedRef.update(_.updatedWith(channel)(_ => Some(true)))
                          _ <- promise.succeed(()).whenZIO(unsubscribedRef.get.map(_.values.forall(identity)))
                        } yield None
                      case _ => ZIO.none
                    }.collectSome
                      .refineToOrDie[RedisError]
                      .interruptWhen(promise)
                  )
    } yield stream

  def pSubscribe[A: BinaryCodec](
    patterns: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit stringCodec: BinaryCodec[String]): IO[RedisError, Stream[RedisError, (String, A)]] =
    for {
      unsubscribedRef <- Ref.make(patterns.map(_ -> false).toMap)
      promise         <- Promise.make[RedisError, Unit]
      patternSet       = patterns.toSet
      stream <- executor
                  .execute(makeCommand(Subscription.PSubscribe, patterns))
                  .map(
                    _.mapZIO(resp => ZIO.attempt(PushProtocolOutput.unsafeDecode(resp))).mapZIO {
                      case PSubscribe(pattern, numOfSubscription) if patternSet contains pattern =>
                        onSubscribe(pattern, numOfSubscription).as(None)
                      case PMessage(pattern, channel, message) if patternSet contains pattern =>
                        ZIO
                          .attempt(ArbitraryOutput[A]().unsafeDecode(message))
                          .map(msg => Some((channel, msg)))
                      case PUnsubscribe(pattern, numOfSubscription) if patternSet contains pattern =>
                        for {
                          _ <- onUnsubscribe(pattern, numOfSubscription)
                          _ <- unsubscribedRef.update(_.updatedWith(pattern)(_ => Some(true)))
                          _ <- promise.succeed(()).whenZIO(unsubscribedRef.get.map(_.values.forall(identity)))
                        } yield None
                      case _ => ZIO.none
                    }.collectSome
                      .refineToOrDie[RedisError]
                      .interruptWhen(promise)
                  )
    } yield stream

  def unsubscribe(channels: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor
      .execute(makeCommand(Subscription.Unsubscribe, channels))
      .flatMap(_.runDrain)

  def pUnsubscribe(patterns: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor
      .execute(makeCommand(Subscription.PUnsubscribe, patterns))
      .flatMap(_.runDrain)

  private def makeCommand(commandName: String, keys: Chunk[String])(implicit codec: BinaryCodec[String]) =
    CommandNameInput.encode(commandName) ++
      Varargs(ArbitraryKeyInput[String]()).encode(keys)
}
