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

package zio.redis.benchmarks

import cats.effect.unsafe.implicits.global
import cats.effect.{IO => CIO, Resource}
import com.comcast.ip4s._
import dev.profunktor.redis4cats.codecs.Codecs
import dev.profunktor.redis4cats.codecs.splits._
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.effect.Log.NoOp.instance
import dev.profunktor.redis4cats.{Redis, RedisCommands}
import io.chrisdavenport.rediculous.RedisConnection
import io.lettuce.core.ClientOptions
import laserdisc.auto.autoRefine
import laserdisc.fs2.RedisClient

trait QueryRunner[Client] {
  def unsafeRunWith(query: Client => CIO[Unit]): Unit
}

object QueryRunner {
  def apply[Client](implicit instance: QueryRunner[Client]): QueryRunner[Client] = instance

  implicit val laserDiscClientRunner: QueryRunner[LaserDiscClient] =
    new QueryRunner[LaserDiscClient] {
      def unsafeRunWith(f: LaserDiscClient => CIO[Unit]): Unit =
        Laserdisc.use(f).unsafeRunSync()
    }

  implicit val rediculousRunner: QueryRunner[RediculousClient] =
    new QueryRunner[RediculousClient] {
      def unsafeRunWith(f: RediculousClient => CIO[Unit]): Unit =
        Rediculous.use(f).unsafeRunSync()
    }

  implicit val redis4CatsStringRunner: QueryRunner[Redis4CatsClient[String]] =
    new QueryRunner[Redis4CatsClient[String]] {
      def unsafeRunWith(f: Redis4CatsClient[String] => CIO[Unit]): Unit =
        Redis4CatsString.use(f).unsafeRunSync()
    }

  implicit val redis4CatsLongRunner: QueryRunner[Redis4CatsClient[Long]] =
    new QueryRunner[Redis4CatsClient[Long]] {
      def unsafeRunWith(f: Redis4CatsClient[Long] => CIO[Unit]): Unit =
        Redis4CatsLong.use(f).unsafeRunSync()
    }

  private[this] final val RedisHost = "127.0.0.1"
  private[this] final val RedisPort = 6379

  private[this] final val Laserdisc: Resource[CIO, LaserDiscClient] = RedisClient[CIO].to(RedisHost, RedisPort)

  private[this] final val Rediculous: Resource[CIO, RediculousClient] =
    RedisConnection
      .queued[CIO]
      .withHost(host"127.0.0.1")
      .withPort(port"6379")
      .withMaxQueued(10000)
      .withWorkers(2)
      .build

  private[this] final val Redis4CatsLong: Resource[CIO, RedisCommands[CIO, String, Long]] = {
    val longCodec = Codecs.derive(RedisCodec.Utf8, stringLongEpi)
    Redis[CIO].withOptions(s"redis://$RedisHost:$RedisPort", ClientOptions.create(), longCodec)
  }

  private[this] final val Redis4CatsString: Resource[CIO, RedisCommands[CIO, String, String]] =
    Redis[CIO].utf8(s"redis://$RedisHost:$RedisPort")
}
