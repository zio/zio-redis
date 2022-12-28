package zio.redis.options

import zio.Chunk

trait PubSub {

  sealed trait ServerPushed {
    def key: String
  }

  object ServerPushed {
    case class Subscribe(channel: String, numOfSubscription: Long) extends ServerPushed {
      def key: String = channel
    }

    case class PSubscribe(pattern: String, numOfSubscription: Long) extends ServerPushed {
      def key: String = pattern
    }
    case class Unsubscribe(channel: String, numOfSubscription: Long) extends ServerPushed {
      def key: String = channel
    }
    case class PUnsubscribe(pattern: String, numOfSubscription: Long) extends ServerPushed {
      def key: String = pattern
    }
    case class Message(channel: String, message: Chunk[Byte]) extends ServerPushed {
      def key: String = channel
    }
    case class PMessage(pattern: String, channel: String, message: Chunk[Byte]) extends ServerPushed {
      def key: String = pattern
    }
  }
  sealed trait SubscriptionKey {
    def key: String
  }

  object SubscriptionKey {
    case class Channel(key: String) extends SubscriptionKey

    case class Pattern(key: String) extends SubscriptionKey
  }

}
