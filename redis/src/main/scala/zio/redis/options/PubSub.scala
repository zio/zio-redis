package zio.redis.options

trait PubSub {

  case class NumSubResponse(channel: String, numOfSubscribers: Long)

  sealed trait PushProtocol[+A] {
    def key: String
  }

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long) extends PushProtocol[Nothing] {
      def key: String = channel
    }
    case class PSubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol[Nothing] {
      def key: String = pattern
    }
    case class Unsubscribe(channel: String, numOfSubscription: Long) extends PushProtocol[Nothing] {
      def key: String = channel
    }
    case class PUnsubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol[Nothing] {
      def key: String = pattern
    }
    case class Message[A](channel: String, message: A) extends PushProtocol[A] {
      def key: String = channel
    }
    case class PMessage[A](pattern: String, channel: String, message: A) extends PushProtocol[A] {
      def key: String = pattern
    }
  }
}
