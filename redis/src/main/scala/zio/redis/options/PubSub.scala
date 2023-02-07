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

  case class NumSubResponse(channel: String, subscriberCount: Long)

  sealed trait PushProtocol {
    def key: SubscriptionKey
  }

  object PushProtocol {
    case class Subscribe(channel: String, numOfSubscription: Long) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Channel(channel)
    }
    case class PSubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Pattern(pattern)
    }
    case class Unsubscribe(channel: String, numOfSubscription: Long) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Channel(channel)
    }
    case class PUnsubscribe(pattern: String, numOfSubscription: Long) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Pattern(pattern)
    }
    case class Message(channel: String, message: RespValue) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Channel(channel)
    }
    case class PMessage(pattern: String, channel: String, message: RespValue) extends PushProtocol {
      def key: SubscriptionKey = SubscriptionKey.Pattern(pattern)
    }
  }
}
