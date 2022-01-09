/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import com.typesafe.config.ConfigFactory
import example.api.Api
import example.config.{AppConfig, ServerConfig}
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{EventLoopGroup, Server}

import zio._
import zio.config.getConfig
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.console._
import zio.logging.Logging
import zio.magic._
import zio.redis.RedisExecutor
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec

object Main extends App {

  private val config = TypesafeConfig.fromTypesafeConfigM[Any, Throwable, AppConfig](
    ZIO.effect(ConfigFactory.load().getConfig("example")),
    AppConfig.descriptor
  )

  private val serverConfig = config.narrow(_.server)
  private val redisConfig  = config.narrow(_.redis)

  private val codec = ZLayer.succeed[Codec](StringUtf8Codec)
  private val redis = Logging.ignore ++ redisConfig ++ codec >>> RedisExecutor.live
  private val sttp  = AsyncHttpClientZioBackend.layer()
  private val cache = redis ++ sttp >>> ContributorsCache.live

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    getConfig[ServerConfig]
      .flatMap(conf =>
        (Server.port(conf.port) ++ Api.routes).make
          .use_(putStrLn("Server online.") *> ZIO.never)
      )
      .injectCustom(
        serverConfig,
        cache,
        ServerChannelFactory.auto,
        EventLoopGroup.auto(0)
      )
      .tapError(e => putStrLn(e.getMessage))
      .exitCode
}
