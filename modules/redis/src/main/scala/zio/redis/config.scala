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

/**
 * Connection settings for one Redis server. `host` and `port` locate the server. With `ssl` set to true, the connection
 * uses TLS, with `sni` as the server name and `verifyCertificate` for the certificate check. `requestQueueSize` is the
 * number of commands that can wait to be sent on the connection; when that number is reached, the next command waits.
 * `auth` holds the credentials that the client sends with `AUTH`.
 *
 * {{{
 * val config: RedisConfig =
 *   RedisConfig("localhost", 6380, auth = Some(RedisConfig.Auth("secret", Some("app"))))
 * }}}
 */
final case class RedisConfig(
  host: String,
  port: Int,
  sni: Option[String] = None,
  ssl: Boolean = false,
  verifyCertificate: Boolean = true,
  requestQueueSize: Int = RedisConfig.DefaultRequestQueueSize,
  auth: Option[RedisConfig.Auth] = None
)

/**
 * Default settings and the credentials type of `RedisConfig`.
 */
object RedisConfig {

  /**
   * The settings for `localhost` on port 6379, the default port of Redis. `Redis.local` and `RedisSubscription.local`
   * connect with them.
   */
  lazy val Local: RedisConfig = RedisConfig("localhost", 6379)

  /**
   * The default `requestQueueSize`, which is 16.
   */
  val DefaultRequestQueueSize: Int = 16

  /**
   * Credentials for `AUTH`. With no `username`, the client sends only the password. A wrong password does not make the
   * layer fail: the next command fails with `ProtocolError`, with the message `Authentication required.`
   */
  final case class Auth(password: String, username: Option[String] = None)
}

/**
 * Connection settings for a Redis cluster. `addresses` are the nodes that the client tries in order to read the cluster
 * layout, and it uses the first one it can connect to. `retry` sets how failed commands are retried. `requestQueueSize`
 * and `auth` apply to each node connection.
 */
final case class RedisClusterConfig(
  addresses: Chunk[RedisUri],
  retry: RetryClusterConfig = RetryClusterConfig.Default,
  requestQueueSize: Int = RedisConfig.DefaultRequestQueueSize,
  auth: Option[RedisConfig.Auth] = None
)

/**
 * How the cluster client retries a command that fails with an `IOError`, `Ask`, `Moved` or `CrossSlot`. The first retry
 * waits `base`, and each later wait is `factor` times the one before. The client makes at most `maxRecurs` retries.
 */
final case class RetryClusterConfig(base: Duration, factor: Double, maxRecurs: Int)

/**
 * Default retry settings for a cluster client.
 */
object RetryClusterConfig {

  /**
   * The default retry settings: a base of 100 milliseconds, a factor of 1.5 and 5 retries.
   */
  lazy val Default: RetryClusterConfig = RetryClusterConfig(100.millis, 1.5, 5)
}
