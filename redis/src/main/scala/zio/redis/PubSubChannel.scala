package zio.redis

import zio.redis.Input.{NonEmptyList, StringInput, Varargs}
import zio.redis.Output.ServerPushedOutput
import zio.schema.codec.BinaryCodec
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, Ref, UIO, ZIO, ZLayer}

trait PubSubChannel {
  def execute(command: RedisPubSubCommand): ZStream[BinaryCodec, RedisError, ServerPushed]
}

final class PubSubChannelImpl(
  activatedKeysRef: Ref[Set[SubscriptionKey]],
  pubSubExecutor: PubSubExecutor
) extends PubSubChannel {
  def execute(command: RedisPubSubCommand): ZStream[BinaryCodec, RedisError, ServerPushed] = {
    import RedisPubSubCommand._

    def makeStream(args: Chunk[RespValue.BulkString])(implicit codec: BinaryCodec) =
      pubSubExecutor
        .execute(args)
        .via(decoder)
        .tap {
          case ServerPushed.Subscribe(channel, _)                 => activateKey(SubscriptionKey.Channel(channel))
          case ServerPushed.PSubscribe(pattern, _)                => activateKey(SubscriptionKey.Pattern(pattern))
          case ServerPushed.Unsubscribe(channel, _)               => inactivateKey(SubscriptionKey.Channel(channel))
          case ServerPushed.PUnsubscribe(pattern, _)              => inactivateKey(SubscriptionKey.Pattern(pattern))
          case _: ServerPushed.Message | _: ServerPushed.PMessage => ZIO.unit
        }

    ZStream.serviceWithStream[BinaryCodec] { implicit codec =>
      val header = StringInput.encode(command.name)
      command match {
        case Subscribe(in) =>
          val params     = NonEmptyList(StringInput).encode(in)
          val args       = header ++ params
          val targetKeys = params.map(_.asString).toSet
          makeStream(args).filter(targetKeys contains _.key)
        case PSubscribe(in) =>
          val params     = NonEmptyList(StringInput).encode(in)
          val args       = header ++ params
          val targetKeys = params.map(_.asString).toSet
          makeStream(args).filter(targetKeys contains _.key)
        case Unsubscribe(in) =>
          val params     = Varargs(StringInput).encode(in)
          val args       = header ++ params
          val targetKeys = params.map(_.asString).toSet
          val filter = { message: ServerPushed =>
            (if (targetKeys.isEmpty) activatedKeysRef.get.map(_.collect { case t: SubscriptionKey.Channel => t.key })
             else ZIO.succeed(targetKeys))
              .map(_ contains message.key)
          }
          makeStream(args).filterZIO(filter)
        case PUnsubscribe(in) =>
          val params     = Varargs(StringInput).encode(in)
          val args       = header ++ params
          val targetKeys = params.map(_.asString).toSet
          val filter = { message: ServerPushed =>
            (if (targetKeys.isEmpty) activatedKeysRef.get.map(_.collect { case t: SubscriptionKey.Pattern => t.key })
             else ZIO.succeed(targetKeys))
              .map(_ contains message.key)
          }
          makeStream(args).filterZIO(filter)
      }
    }
  }

  private def inactivateKey(key: SubscriptionKey): UIO[Unit] =
    for {
      channelKeys <- activatedKeysRef.get
      _           <- activatedKeysRef.set(channelKeys - key).when(channelKeys contains key)
    } yield ()

  private def activateKey(key: SubscriptionKey): UIO[Unit] =
    for {
      channelKeys <- activatedKeysRef.get
      _           <- activatedKeysRef.set(channelKeys + key).unless(channelKeys contains key)
    } yield ()

  private def decoder(implicit codec: BinaryCodec): ZPipeline[Any, RedisError, RespValue, ServerPushed] =
    ZPipeline.mapZIO(resp => ZIO.attempt(ServerPushedOutput.unsafeDecode(resp)).refineToOrDie[RedisError])

}
object PubSubChannel {
  lazy val layer: ZLayer[PubSubExecutor, RedisError.IOError, PubSubChannel] =
    ZLayer.fromZIO(
      for {
        executor <- ZIO.service[PubSubExecutor]
        ref      <- Ref.make(Set.empty[SubscriptionKey])
      } yield new PubSubChannelImpl(ref, executor)
    )
}
