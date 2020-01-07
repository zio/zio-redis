package zio.redis

import zio.RIO
import zio.redis.R._

trait HashApi {

  def hget(key: String, field: String): RIO[RedisIO, Option[String]] =
    send("HGET", key, field)(asBulk[String])

  def hdel(key: String, field: String, rest: String*): RIO[RedisIO, Option[Int]] =
    send("HDEL", Seq(RedisArgMagnet(key), RedisArgMagnet(field)) ++ rest.map(RedisArgMagnet(_)): _*)(asInt)

  def hexists(key: String, field: String): RIO[RedisIO, Boolean] =
    send("HEXISTS", key, field)(asBoolean)

  def hgetall[K: Parse, V: Parse](key: String): RIO[RedisIO, Map[K, V]] =
    send("HGETALL", key)(asListPairs[K, V].map(_.flatten.toMap))
}
