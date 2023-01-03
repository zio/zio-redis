package zio.redis.options

trait PubSub {

  case class NumSubResponse(channel: String, numOfSubscribers: Long)

  sealed trait PushProtocol[+A]

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long)       extends PushProtocol[Nothing]
    case class PSubscribe(pattern: String, numOfSubscription: Long)      extends PushProtocol[Nothing]
    case class Unsubscribe(channel: String, numOfSubscription: Long)     extends PushProtocol[Nothing]
    case class PUnsubscribe(pattern: String, numOfSubscription: Long)    extends PushProtocol[Nothing]
    case class Message[A](channel: String, message: A)                   extends PushProtocol[A]
    case class PMessage[A](pattern: String, channel: String, message: A) extends PushProtocol[A]
  }
}
