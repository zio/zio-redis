package zio.redis

final case class RedisConfig(host: String, port: Int)

object RedisConfig {
  lazy val Default = RedisConfig("localhost", 6379)
}
