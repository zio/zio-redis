package example.config

import zio.config.ConfigDescriptor
import zio.config.magnolia.DeriveConfigDescriptor
import zio.redis.RedisConfig

final case class AppConfig(redis: RedisConfig, server: ServerConfig)

object AppConfig {
  val descriptor: ConfigDescriptor[AppConfig] = DeriveConfigDescriptor.descriptor[AppConfig]
}
