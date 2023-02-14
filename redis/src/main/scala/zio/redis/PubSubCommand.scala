package zio.redis

import zio.IO

sealed trait PubSubCommand

private[redis] object PubSubCommand {
  case class Subscribe(
    channel: String,
    channels: List[String],
    callback: PushProtocol => IO[RedisError, Unit]
  ) extends PubSubCommand
  case class PSubscribe(
    pattern: String,
    patterns: List[String],
    callback: PushProtocol => IO[RedisError, Unit]
  ) extends PubSubCommand
  case class Unsubscribe(
    channels: List[String],
    callback: PushProtocol => IO[RedisError, Unit]
  ) extends PubSubCommand
  case class PUnsubscribe(
    patterns: List[String],
    callback: PushProtocol => IO[RedisError, Unit]
  ) extends PubSubCommand
}
