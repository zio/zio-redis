package example

import akka.actor.ActorSystem
import akka.http.interop._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import example.api.Api
import example.config.AppConfig
import example.domain.ContributorService.SttpClient
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio._
import zio.console._
import zio.redis._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig

object Main extends App {

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO(ConfigFactory.load.resolve)
      .flatMap(rawConfig => program.provideCustomLayer(prepareEnvironment(rawConfig)))
      .exitCode

  private val program: RIO[HttpServer with ZEnv, Unit] = {
    val startHttpServer = HttpServer.start.tapM(_ => putStrLn("Server online."))

    startHttpServer.useForever
  }

  private def prepareEnvironment(rawConfig: Config): TaskLayer[HttpServer] = {
    val configLayer = TypesafeConfig.fromTypesafeConfig(rawConfig, AppConfig.descriptor)

    val actorSystemLayer: TaskLayer[Has[ActorSystem]] = ZLayer.fromManaged {
      ZManaged.make(ZIO(ActorSystem("zio-redis-example")))(system => ZIO.fromFuture(_ => system.terminate()).either)
    }

    val apiConfigLayer = configLayer.narrow(_.api)
    val redisConfigLayer = configLayer.narrow(_.redis) // ???

    val sttpLayer: TaskLayer[SttpClient] = AsyncHttpClientZioBackend().toLayer
    val apiLayer: TaskLayer[Api] = (apiConfigLayer ++ redisConfigLayer ++ sttpLayer) >>> Api.live
    val routesLayer: URLayer[Api, Has[Route]] = ZLayer.fromService(_.routes)

    val redisLayer = RedisExecutor.live("localhost", 1)

    val serverEnv: TaskLayer[HttpServer] =
      (actorSystemLayer ++ apiConfigLayer ++ (apiLayer >>> routesLayer)) >>> HttpServer.live

    serverEnv
  }
}
