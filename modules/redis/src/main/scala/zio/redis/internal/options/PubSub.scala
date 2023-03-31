package zio.redis.options

import zio.UIO
import zio.redis.internal.RespValue
object PubSub {
  type PubSubCallback = (String, Long) => UIO[Unit]

  private[redis] sealed trait PushProtocol

  private[redis] object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long)            extends PushProtocol
    case class PSubscribe(pattern: String, numOfSubscription: Long)           extends PushProtocol
    case class Unsubscribe(channel: String, numOfSubscription: Long)          extends PushProtocol
    case class PUnsubscribe(pattern: String, numOfSubscription: Long)         extends PushProtocol
    case class Message(channel: String, message: RespValue)                   extends PushProtocol
    case class PMessage(pattern: String, channel: String, message: RespValue) extends PushProtocol
  }

  final case class NumberOfSubscribers(channel: String, subscriberCount: Long)
}
