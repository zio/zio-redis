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

import cats.effect.unsafe.implicits.global
import cats.effect.{IO => CatsIO, _}
import com.comcast.ip4s._
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.data.RedisCodec
import fs2.io.net.Network
import io.chrisdavenport.rediculous.RedisConnection
import io.lettuce.core.ClientOptions
import laserdisc.fs2.RedisClient

trait RedisClients {
  import RedisClients._

  type Redis4CatsClient[V] = RedisCommands[CatsIO, String, V]
  type LaserDiscClient     = RedisClient[CatsIO]
  type RediculousClient    = RedisConnection[CatsIO]

  trait QueryUnsafeRunner[F] {
    def unsafeRun(f: F => CatsIO[Unit]): Unit
  }

  implicit object LaserDiscClientRunner extends QueryUnsafeRunner[LaserDiscClient] {
    override def unsafeRun(f: LaserDiscClient => CatsIO[Unit]): Unit = laserDiskConnection.use(f).unsafeRunSync()
  }

  implicit object RedicoulusClientRunner extends QueryUnsafeRunner[RediculousClient] {
    override def unsafeRun(f: RediculousClient => CatsIO[Unit]): Unit = redicoulusConnection.use(f).unsafeRunSync()
  }

  implicit object Redis4CatsClientRunnerString extends QueryUnsafeRunner[Redis4CatsClient[String]] {
    override def unsafeRun(f: Redis4CatsClient[String] => CatsIO[Unit]): Unit =
      redis4CatsConnectionString.use(f).unsafeRunSync()
  }

  implicit object Redis4CatsClientRunnerLong extends QueryUnsafeRunner[Redis4CatsClient[Long]] {
    override def unsafeRun(f: Redis4CatsClient[Long] => CatsIO[Unit]): Unit =
      redis4CatsConnectionLong.use(f).unsafeRunSync()
  }

  private val redicoulusConnection: Resource[CatsIO, RediculousClient] =
    RedisConnection.queued[CatsIO](Network[CatsIO], host"127.0.0.1", port"6379", maxQueued = 10000, workers = 2)

  import _root_.laserdisc.auto.autoRefine
  private val laserDiskConnection: Resource[CatsIO, LaserDiscClient] = RedisClient[CatsIO].to(RedisHost, RedisPort)

  import dev.profunktor.redis4cats.Redis
  import dev.profunktor.redis4cats.effect.Log.NoOp.instance
  private val redis4CatsConnectionString: Resource[CatsIO, RedisCommands[CatsIO, String, String]] =
    Redis[CatsIO].utf8(s"redis://$RedisHost:$RedisPort")

  private val redis4CatsConnectionLong: Resource[CatsIO, RedisCommands[CatsIO, String, Long]] = {
    import dev.profunktor.redis4cats.codecs.Codecs
    import dev.profunktor.redis4cats.codecs.splits._

    val longCodec: RedisCodec[String, Long] = Codecs.derive(RedisCodec.Utf8, stringLongEpi)
    Redis[CatsIO].withOptions(s"redis://$RedisHost:$RedisPort", ClientOptions.create(), longCodec)
  }
}

object RedisClients {
  private final val RedisHost = "127.0.0.1"
  private final val RedisPort = 6379
}
