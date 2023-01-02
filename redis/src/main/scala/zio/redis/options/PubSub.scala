package zio.redis.options

import zio.Chunk

trait PubSub {

  case class NumSubResponse(channel: String, numOfSubscribers: Long)

  sealed trait PushProtocol

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long)              extends PushProtocol
    case class PSubscribe(pattern: String, numOfSubscription: Long)             extends PushProtocol
    case class Unsubscribe(channel: String, numOfSubscription: Long)            extends PushProtocol
    case class PUnsubscribe(pattern: String, numOfSubscription: Long)           extends PushProtocol
    case class Message(channel: String, message: Chunk[Byte])                   extends PushProtocol
    case class PMessage(pattern: String, channel: String, message: Chunk[Byte]) extends PushProtocol
  }
}
