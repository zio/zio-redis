package zio.redis

private[redis] sealed trait PubSubCommand

private[redis] object PubSubCommand {
  case class Subscribe(
    channel: String,
    channels: List[String]
  ) extends PubSubCommand
  case class PSubscribe(
    pattern: String,
    patterns: List[String]
  ) extends PubSubCommand
  case class Unsubscribe(channels: List[String])  extends PubSubCommand
  case class PUnsubscribe(patterns: List[String]) extends PubSubCommand
}
