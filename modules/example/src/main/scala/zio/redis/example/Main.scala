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

package zio.redis.example

import com.typesafe.config.ConfigFactory
import sttp.client3.httpclient.zio.HttpClientZioBackend
import zio._
import zio.config.typesafe.TypesafeConfigProvider
import zio.http.Server
import zio.redis._
import zio.redis.example.api.Api
import zio.redis.example.config.AppConfig
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

object Main extends ZIOAppDefault {

  override val bootstrap: Layer[Nothing, Unit] = {
    val provider = TypesafeConfigProvider.fromTypesafeConfig(ConfigFactory.load.getConfig("example"))
    Runtime.setConfigProvider(provider)
  }

  def run: ZIO[ZIOAppArgs with Scope, Any, ExitCode] =
    Server
      .serve(Api.routes)
      .provide(
        Server.defaultWithPort(9000),
        AppConfig.layer,
        ContributorsCache.layer,
        HttpClientZioBackend.layer(),
        Redis.singleNode,
        ZLayer.succeed(new CodecSupplier {
          def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
        })
      )
      .exitCode
}
