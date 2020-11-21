package github.contributors

import akka.actor.ActorSystem
import akka.http.interop._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import github.contributors.api.{Api, SttpClient}
import github.contributors.config.AppConfig
import sttp.client.SttpBackend
import sttp.client.asynchttpclient.WebSocketHandler
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio._
import zio.console._
import zio.redis._
import zio.config.typesafe.TypesafeConfig
import zio.logging.{LogAnnotation, Logging}

object Main extends App {

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO(ConfigFactory.load.resolve)
      .flatMap(rawConfig => program.provideCustomLayer(prepareEnvironment(rawConfig)))
      .exitCode

  private val program: RIO[HttpServer with ZEnv, Unit] = {
    val startHttpServer = HttpServer.start.tapM(_ => putStrLn("Server online."))
    val redisExecutor = RedisExecutor.live("localhost", 1)

    startHttpServer.useForever
  }

  private def prepareEnvironment(rawConfig: Config): TaskLayer[HttpServer] = {
    val configLayer = TypesafeConfig.fromTypesafeConfig(rawConfig, AppConfig.descriptor)

    val actorSystemLayer: TaskLayer[Has[ActorSystem]] = ZLayer.fromManaged {
      ZManaged.make(ZIO(ActorSystem("zio-redis-example")))(system => ZIO.fromFuture(_ => system.terminate()).either)
    }

    val apiConfigLayer = configLayer.map(config => Has(config.get.api))
    val redisConfigLayer = configLayer.map(config => Has(config.get.redis))

    val sttpLayer: TaskLayer[SttpClient] = AsyncHttpClientZioBackend().toLayer
    val apiLayer: TaskLayer[Api] = (apiConfigLayer ++ sttpLayer) >>> Api.live
    val routesLayer: URLayer[Api, Has[Route]] = ZLayer.fromService(_.routes)

    val serverEnv: TaskLayer[HttpServer] =
      (actorSystemLayer ++ apiConfigLayer ++ (apiLayer >>> routesLayer)) >>> HttpServer.live

    serverEnv
  }
}
