package example.config

import akka.http.interop.HttpServer

import zio.config.ConfigDescriptor
import zio.config.magnolia.DeriveConfigDescriptor
import zio.redis.RedisConfig

final case class AppConfig(redis: RedisConfig, server: HttpServer.Config)

object AppConfig {
  val descriptor: ConfigDescriptor[AppConfig] = DeriveConfigDescriptor.descriptor[AppConfig]
}
