package zio.redis

import zio.schema.codec.BinaryCodec
import zio.stream._
import zio.{IO, ZLayer}

final case class RedisPubSubCommand(command: PubSubCommand, codec: BinaryCodec, executor: RedisPubSub) {
  def run: IO[RedisError, List[Stream[RedisError, PushProtocol]]] = {
    val codecLayer = ZLayer.succeed(codec)
    executor.execute(command).provideLayer(codecLayer)
  }
}

sealed trait PubSubCommand

object PubSubCommand {
  case class Subscribe(channel: String, channels: List[String])  extends PubSubCommand
  case class PSubscribe(pattern: String, patterns: List[String]) extends PubSubCommand
  case class Unsubscribe(channels: List[String])                 extends PubSubCommand
  case class PUnsubscribe(patterns: List[String])                extends PubSubCommand
}
