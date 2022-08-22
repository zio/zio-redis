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
import zio.Console.printLine
import zio._
import zio.config.getConfig
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig
import zio.redis.{Redis, RedisExecutor}
import zio.schema.codec.{Codec, ProtobufCodec}

object Main extends ZIOAppDefault {
  private val config =
    TypesafeConfig.fromTypesafeConfig(ConfigFactory.load().getConfig("example"), AppConfig.confDescriptor)

  private val serverConfig = config.narrow(_.server)
  private val redisConfig  = config.narrow(_.redis)

  private val codec         = ZLayer.succeed[Codec](ProtobufCodec)
  private val redisExecutor = redisConfig >>> RedisExecutor.live
  private val redis         = redisExecutor ++ codec >>> Redis.live
  private val sttp          = AsyncHttpClientZioBackend.layer()
  private val cache         = redis ++ sttp >>> ContributorsCache.ServiceLive.layer

  def run: ZIO[ZIOAppArgs with Scope, Any, ExitCode] =
    getConfig[ServerConfig]
      .flatMap(conf =>
        (Server.port(conf.port) ++ Api.routes).make.flatMap(_ => printLine("Server online.") *> ZIO.never)
      )
      .provideSome[Scope](
        serverConfig,
        cache,
        ServerChannelFactory.auto,
        EventLoopGroup.auto(0)
      )
      .tapError(e => printLine(e.getMessage))
      .exitCode
}
