package example

import akka.actor.ActorSystem
import akka.http.interop._
import akka.http.scaladsl.server.Route
import com.typesafe.config.{Config, ConfigFactory}
import example.api.Api
import example.config.AppConfig
import example.domain.ContributorService
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import zio._
import zio.redis.RedisExecutor
import zio.console._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.logging.Logging

object Main extends App {

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    ZIO(ConfigFactory.load.resolve)
      .flatMap(rawConfig => program.provideCustomLayer(prepareEnvironment(rawConfig)))
      .exitCode

  private val program: RIO[HttpServer with ZEnv, Unit] =
    HttpServer.start.tapM(_ => putStrLn("Server online.")).useForever

  private def prepareEnvironment(rawConfig: Config): TaskLayer[HttpServer] = {
    val configLayer = TypesafeConfig.fromTypesafeConfig(rawConfig, AppConfig.descriptor)

    val actorSystemLayer =
      ZManaged.make(ZIO(ActorSystem("zio-redis-example")))(system => ZIO.fromFuture(_ => system.terminate()).either).toLayer

    val apiConfigLayer = configLayer.narrow(_.api)
    val redisConfigLayer = configLayer.narrow(_.redis)

    val redisLayer = Logging.ignore >>> RedisExecutor.loopback().orDie
    val sttpLayer = AsyncHttpClientZioBackend.layer()

    val contributorLayer = redisLayer ++ sttpLayer >>> ContributorService.live
    val apiLayer = contributorLayer >>> Api.live
    val routesLayer = ZLayer.fromService[Api.Service, Route](_.routes)


    (actorSystemLayer ++ apiConfigLayer ++ (apiLayer >>> routesLayer)) >>> HttpServer.live
  }
}
