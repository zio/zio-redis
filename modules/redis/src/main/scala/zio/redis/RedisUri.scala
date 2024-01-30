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

final case class RedisUri(host: String, port: Int, ssl: Boolean, sni: Option[String]) {
  override def toString: String = ssl match {
    case true  => s"rediss://$host:$port"
    case false => s"redis://$host:$port"
  }
}

object RedisUri {
  def apply(hostAndPort: String): RedisUri = {
    val splitting = hostAndPort.split(':')
    val host      = splitting(0)
    val port      = splitting(1).toInt
    RedisUri(host, port, false, None)
  }
}
