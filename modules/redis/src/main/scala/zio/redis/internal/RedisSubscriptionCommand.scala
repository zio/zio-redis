package zio.redis.internal

import zio.redis.Input._
import zio.redis.Output.ArbitraryOutput
import zio.redis._
import zio.redis.api.Subscription
import zio.redis.options.PubSub.PubSubCallback
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, IO, ZIO}

private[redis] final case class RedisSubscriptionCommand(executor: SubscriptionExecutor) extends {
  import zio.redis.options.PubSub.PushProtocol._

  def subscribe[A: BinaryCodec](
    channels: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit codec: BinaryCodec[String]): Stream[RedisError, (String, A)] =
    executor
      .execute(makeCommand(Subscription.Subscribe, channels))
      .mapZIO {
        case Subscribe(channel, numOfSubscription) =>
          onSubscribe(channel, numOfSubscription).as(None)
        case Message(channel, message) =>
          ZIO
            .attempt(ArbitraryOutput[A]().unsafeDecode(message))
            .map(msg => Some((channel, msg)))
        case Unsubscribe(channel, numOfSubscription) =>
          onUnsubscribe(channel, numOfSubscription).as(None)
        case _ => ZIO.none
      }
      .collectSome
      .refineToOrDie[RedisError]

  def pSubscribe[A: BinaryCodec](
    patterns: Chunk[String],
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  )(implicit stringCodec: BinaryCodec[String]): Stream[RedisError, (String, A)] =
    executor
      .execute(makeCommand(Subscription.PSubscribe, patterns))
      .mapZIO {
        case PSubscribe(pattern, numOfSubscription) =>
          onSubscribe(pattern, numOfSubscription).as(None)
        case PMessage(_, channel, message) =>
          ZIO
            .attempt(ArbitraryOutput[A]().unsafeDecode(message))
            .map(msg => Some((channel, msg)))
        case PUnsubscribe(pattern, numOfSubscription) =>
          onUnsubscribe(pattern, numOfSubscription).as(None)
        case _ => ZIO.none
      }
      .collectSome
      .refineToOrDie[RedisError]

  def unsubscribe(channels: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor
      .execute(makeCommand(Subscription.Unsubscribe, channels))
      .runDrain

  def pUnsubscribe(patterns: Chunk[String])(implicit codec: BinaryCodec[String]): IO[RedisError, Unit] =
    executor
      .execute(makeCommand(Subscription.PUnsubscribe, patterns))
      .runDrain

  private def makeCommand(commandName: String, keys: Chunk[String])(implicit codec: BinaryCodec[String]) =
    CommandNameInput.encode(commandName) ++
      Varargs(ArbitraryKeyInput[String]()).encode(keys)
}
