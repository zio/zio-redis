package example

import com.typesafe.config.ConfigFactory
import example.api.Api
import example.config.{ AppConfig, ServerConfig }
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ EventLoopGroup, Server }
import zio._
import zio.config.getConfig
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.console._
import zio.logging.Logging
import zio.magic._
import zio.redis.codec.StringUtf8Codec
import zio.redis.RedisExecutor
import zio.schema.codec.Codec

object Main extends App {

  private val config = TypesafeConfig.fromTypesafeConfigM[Any, Throwable, AppConfig](
    ZIO.effect(ConfigFactory.load().getConfig("example")),
    AppConfig.descriptor
  )

  val serverConfig = config.narrow(_.server)
  val redisConfig  = config.narrow(_.redis)

  val codec = ZLayer.succeed[Codec](StringUtf8Codec)
  val redis = Logging.ignore ++ redisConfig ++ codec >>> RedisExecutor.live
  val sttp  = AsyncHttpClientZioBackend.layer()
  val cache = redis ++ sttp >>> ContributorsCache.live

  val runServer =
    getConfig[ServerConfig]
      .flatMap(conf =>
        (Server.port(conf.port) ++ Api.server).make
          .use_(putStrLn("Server online.") *> ZIO.never)
      )

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    runServer
      .injectCustom(
        serverConfig,
        cache,
        ServerChannelFactory.auto,
        EventLoopGroup.auto(0)
      )
      .tapError(e => putStrLn(e.getMessage))
      .exitCode
}
