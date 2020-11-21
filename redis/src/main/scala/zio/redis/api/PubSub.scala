package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{ Chunk, ZIO }

trait PubSub {
  import PubSub._

  final def pSubscribe(a: String, as: String*): ZIO[RedisExecutor, RedisError, Unit] = PSubscribe.run((a, as.toList))

  final def pubSubChannels(a: String): ZIO[RedisExecutor, RedisError, Chunk[String]] =
    PubSubChannels.run(("CHANNELS", List(a)))

  final def pubSubNumSub(a: String, as: String*): ZIO[RedisExecutor, RedisError, Map[String, Long]] =
    PubSubNumSub.run(("NUMSUB", a :: as.toList))

  final def pubSubNumPat(): ZIO[RedisExecutor, RedisError, Long] = PubSubNumPat.run("NUMPAT")

  final def publish(a: String, b: String): ZIO[RedisExecutor, RedisError, Long] = Publish.run((a, b))

  final def pUnSubscribe(as: String*): ZIO[RedisExecutor, RedisError, Unit] = PUnSubscribe.run(as.toList)

  final def subscribe(a: String, as: String*): ZIO[RedisExecutor, RedisError, Unit] = Subscribe.run((a, as.toList))

  final def unSubscribe(as: String*): ZIO[RedisExecutor, RedisError, Unit] = UnSubscribe.run(as.toList)

}

private object PubSub {
  final val PSubscribe     = RedisCommand("PSUBSCRIBE", NonEmptyList(StringInput), UnitOutput)
  final val PubSubChannels = RedisCommand("PUBSUB", NonEmptyList(StringInput), ChunkOutput)
  final val PubSubNumSub   = RedisCommand("PUBSUB", NonEmptyList(StringInput), KeyLongValueOutput)
  final val PubSubNumPat   = RedisCommand("PUBSUB", StringInput, LongOutput)
  final val Publish        = RedisCommand("PUBLISH", Tuple2(StringInput, StringInput), LongOutput)
  final val PUnSubscribe   = RedisCommand("PUNSUBSCRIBE", ListInput(StringInput), UnitOutput)
  final val Subscribe      = RedisCommand("SUBSCRIBE", NonEmptyList(StringInput), UnitOutput)
  final val UnSubscribe    = RedisCommand("UNSUBSCRIBE", ListInput(StringInput), UnitOutput)

}
