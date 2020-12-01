package example.config

import akka.http.interop.HttpServer

import zio.config.magnolia.DeriveConfigDescriptor

final case class AppConfig(api: HttpServer.Config, redis: RedisConfig)

object AppConfig {
  val descriptor = DeriveConfigDescriptor.descriptor[AppConfig]
}
