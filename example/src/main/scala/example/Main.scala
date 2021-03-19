package example

import akka.actor.ActorSystem
import akka.http.interop._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{ Config, ConfigFactory }
import example.api.Api
import example.config.AppConfig
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend

import zio._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.console._
import zio.logging.Logging
import zio.redis.RedisExecutor
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec

object Main extends App {

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO
      .effect(ConfigFactory.load().getConfig("example"))
      .map(makeLayer)
      .flatMap(runServer)
      .exitCode

  private def makeLayer(rawConfig: Config): TaskLayer[HttpServer] = {
    val config       = TypesafeConfig.fromTypesafeConfig[AppConfig](rawConfig, AppConfig.descriptor)
    val serverConfig = config.narrow(_.server)
    val redisConfig  = config.narrow(_.redis)

    val actorSystem =
      ZManaged
        .make(ZIO.succeed(ActorSystem("zio-redis-example")))(as => ZIO.fromFuture(_ => as.terminate()).either)
        .toLayer

    val codec  = ZLayer.succeed[Codec](StringUtf8Codec)
    val redis  = Logging.ignore ++ redisConfig ++ codec >>> RedisExecutor.live
    val sttp   = AsyncHttpClientZioBackend.layer()
    val cache  = redis ++ sttp >>> ContributorsCache.live
    val api    = cache >>> Api.live
    val routes = ZLayer.fromService[Api.Service, Route](_.routes)

    (actorSystem ++ serverConfig ++ (api >>> routes)) >>> HttpServer.live
  }

  private def runServer(layer: TaskLayer[HttpServer]): RIO[ZEnv, Nothing] =
    HttpServer.start.tapM(_ => putStrLn("Server online.")).useForever.provideCustomLayer(layer)
}
