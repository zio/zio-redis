package zio.redis

import zio.ZLayer
import zio.stream.ZStream

sealed abstract class RedisPubSubCommand

object RedisPubSubCommand {
  case class Subscribe(channel: String, channels: List[String])  extends RedisPubSubCommand
  case class PSubscribe(pattern: String, patterns: List[String]) extends RedisPubSubCommand
  case class Unsubscribe(channels: List[String])                 extends RedisPubSubCommand
  case class PUnsubscribe(patterns: List[String])                extends RedisPubSubCommand

  def run(command: RedisPubSubCommand): ZStream[Redis, RedisError, PushProtocol] =
    ZStream.serviceWithStream { redis =>
      val codecLayer = ZLayer.succeed(redis.codec)
      redis.pubSub.execute(command).provideLayer(codecLayer)
    }
}
