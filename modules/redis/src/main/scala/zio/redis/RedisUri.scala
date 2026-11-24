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

/**
 * The address of one Redis server. `host` and `port` locate it. With `ssl` set to true, the connection uses TLS, and
 * `sni` is the server name sent in the TLS handshake. With `verifyCertificate` set to true, the certificate is checked
 * against the default trust store and the host name. With it set to false, any certificate is accepted.
 *
 * `toString` gives `redis://host:port`, or `rediss://host:port` when `ssl` is set. It leaves out `sni` and
 * `verifyCertificate`.
 */
final case class RedisUri(
  host: String,
  port: Int,
  ssl: Boolean,
  sni: Option[String],
  verifyCertificate: Boolean = true
) {
  override def toString: String =
    if (ssl) s"rediss://$host:$port" else s"redis://$host:$port"
}

/**
 * Creates `RedisUri` values from a `host:port` string, or from a host and a port.
 */
object RedisUri {
  /**
   * Parses a `host:port` string such as `localhost:6379`, with TLS off. The string is split at each colon, and only the
   * first two parts are used. A string without a colon fails with `ArrayIndexOutOfBoundsException`. A port that is not
   * a number fails with `NumberFormatException`, and so does an IPv6 address in brackets.
   */
  def apply(hostAndPort: String): RedisUri = {
    val splitting = hostAndPort.split(':')
    val host      = splitting(0)
    val port      = splitting(1).toInt
    RedisUri(host, port, ssl = false, sni = None)
  }

  /**
   * Parses a `host:port` string, as the one-argument form does, with TLS set by `ssl`.
   */
  def apply(hostAndPort: String, ssl: Boolean): RedisUri = {
    val splitting = hostAndPort.split(':')
    val host      = splitting(0)
    val port      = splitting(1).toInt
    RedisUri(host, port, ssl = ssl, sni = None)
  }

  /**
   * Parses a `host:port` string with TLS set by `ssl` and certificate checking set by `verifyCertificate`. The failures
   * are the same as for the one-argument form.
   */
  def apply(hostAndPort: String, ssl: Boolean, verifyCertificate: Boolean): RedisUri = {
    val splitting = hostAndPort.split(':')
    val host      = splitting(0)
    val port      = splitting(1).toInt
    RedisUri(host, port, ssl = ssl, sni = None, verifyCertificate = verifyCertificate)
  }

  /**
   * Creates a `RedisUri` for `host` and `port`, with TLS off.
   */
  def apply(host: String, port: Int): RedisUri =
    RedisUri(host, port, ssl = false, sni = None)

  /**
   * Creates a `RedisUri` for `host` and `port`, with TLS set by `ssl`.
   */
  def apply(host: String, port: Int, ssl: Boolean): RedisUri =
    RedisUri(host, port, ssl = ssl, sni = None)

  /**
   * Creates a `RedisUri` for `host` and `port`, with TLS set by `ssl` and certificate checking set by
   * `verifyCertificate`.
   */
  def apply(host: String, port: Int, ssl: Boolean, verifyCertificate: Boolean): RedisUri =
    RedisUri(host, port, ssl = ssl, sni = None, verifyCertificate = verifyCertificate)
}
