package zio.redis.options

import zio.IO
import zio.redis.{RedisError, RespValue}
trait PubSub {
  type PubSubCallback = (String, Long) => IO[RedisError, Unit]

  sealed trait SubscriptionKey { self =>
    def value: String

    def isChannelKey = self match {
      case _: SubscriptionKey.Channel => true
      case _: SubscriptionKey.Pattern => false
    }

    def isPatternKey = !isChannelKey
  }

  object SubscriptionKey {
    case class Channel(value: String) extends SubscriptionKey
    case class Pattern(value: String) extends SubscriptionKey
  }

  sealed trait PushProtocol {
    def key: String
  }

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long) extends PushProtocol {
      def key: String = channel
    }
    case class PSubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol {
      def key: String = pattern
    }
    case class Unsubscribe(channel: String, numOfSubscription: Long) extends PushProtocol {
      def key: String = channel
    }
    case class PUnsubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol {
      def key: String = pattern
    }
    case class Message(channel: String, message: RespValue) extends PushProtocol {
      def key: String = channel
    }
    case class PMessage(pattern: String, channel: String, message: RespValue) extends PushProtocol {
      def key: String = pattern
    }
  }
}

object PubSub {
  final case class NumberOfSubscribers(channel: String, subscriberCount: Long)
}
