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

package zio.redis.example.config

import com.typesafe.config.ConfigFactory
import zio.Config.{Error => ConfigError}
import zio._
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider
import zio.redis.RedisConfig

final case class AppConfig(redis: RedisConfig)

object AppConfig {
  type Env = AppConfig with RedisConfig

  private[this] final val Config     = ConfigFactory.load.getConfig("example")
  private[this] final val Descriptor = deriveConfig[AppConfig]

  lazy val layer: Layer[ConfigError, Env] = {
    val config = TypesafeConfigProvider.fromTypesafeConfig(Config).load(Descriptor)
    ZLayer.fromZIO(config) ++ ZLayer.fromZIO(config.map(_.redis))
  }
}
