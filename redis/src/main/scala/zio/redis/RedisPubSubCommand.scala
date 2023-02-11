package zio.redis

import zio.redis.Output.ArbitraryOutput
import zio.schema.Schema
import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{Chunk, IO, ZIO, ZLayer}

final case class RedisPubSubCommand(command: PubSubCommand, codec: BinaryCodec, executor: RedisPubSub) {
  def run[R: Schema](
    callback: PushProtocol => IO[RedisError, Unit]
  ): IO[RedisError, Chunk[Stream[RedisError, R]]] = {
    val codecLayer = ZLayer.succeed(codec)
    executor
      .execute(command)
      .provideLayer(codecLayer)
      .map(
        _.map(
          _.tap(callback(_)).mapZIO {
            case PushProtocol.Message(_, msg) =>
              ZIO
                .attempt(ArbitraryOutput[R]().unsafeDecode(msg)(codec))
                .refineToOrDie[RedisError]
                .asSome
            case PushProtocol.PMessage(_, _, msg) =>
              ZIO
                .attempt(ArbitraryOutput[R]().unsafeDecode(msg)(codec))
                .refineToOrDie[RedisError]
                .asSome
            case _ => ZIO.none
          }.collectSome
        )
      )
  }
}
