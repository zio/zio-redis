package zio.redis.options

import zio.IO
import zio.redis.{RedisError, RespValue}
trait PubSub {
  type PubSubCallback = (String, Long) => IO[RedisError, Unit]

  sealed trait PushProtocol

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long)            extends PushProtocol
    case class PSubscribe(pattern: String, numOfSubscription: Long)           extends PushProtocol
    case class Unsubscribe(channel: String, numOfSubscription: Long)          extends PushProtocol
    case class PUnsubscribe(pattern: String, numOfSubscription: Long)         extends PushProtocol
    case class Message(channel: String, message: RespValue)                   extends PushProtocol
    case class PMessage(pattern: String, channel: String, message: RespValue) extends PushProtocol
  }
}

object PubSub {
  final case class NumberOfSubscribers(channel: String, subscriberCount: Long)
}
