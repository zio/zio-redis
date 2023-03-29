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

package zio.redis

import zio.{Chunk, Duration, durationInt}

final case class RedisConfig(host: String, port: Int)

object RedisConfig {
  lazy val Local: RedisConfig = RedisConfig("localhost", 6379)
}

final case class RedisClusterConfig(addresses: Chunk[RedisUri], retry: RetryClusterConfig = RetryClusterConfig.Default)

final case class RetryClusterConfig(base: Duration, factor: Double, maxRecurs: Int)

object RetryClusterConfig {
  lazy val Default: RetryClusterConfig = RetryClusterConfig(100.millis, 1.5, 5)
}
