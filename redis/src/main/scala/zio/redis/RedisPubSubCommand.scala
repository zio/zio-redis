package zio.redis

import zio.ZEnvironment
import zio.stream._

sealed abstract class RedisPubSubCommand(val name: String) {
  private[redis] def run: ZStream[Redis, RedisError, ServerPushed] =
    ZStream.serviceWithStream[Redis] { redis =>
      redis.pubSub.execute(this).provideEnvironment(ZEnvironment(redis.codec))
    }
}

object RedisPubSubCommand {
  case class Subscribe(in: (String, List[String]))  extends RedisPubSubCommand("SUBSCRIBE")
  case class Unsubscribe(in: List[String])          extends RedisPubSubCommand("UNSUBSCRIBE")
  case class PSubscribe(in: (String, List[String])) extends RedisPubSubCommand("PSUBSCRIBE")
  case class PUnsubscribe(in: List[String])         extends RedisPubSubCommand("PUNSUBSCRIBE")
}
