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

import cats.effect._
import cats.effect.unsafe.IORuntime
import dev.profunktor.redis4cats.codecs.Codecs
import dev.profunktor.redis4cats.codecs.splits._
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.effect.Log.NoOp.instance
import dev.profunktor.redis4cats.{Redis, RedisCommands}
import io.lettuce.core.ClientOptions

private[benchmarks] final class Redis4Cats private (
  client: RedisCommands[IO, String, Long],
  finalizer: IO[Unit]
)(implicit runtime: IORuntime) {
  def run(program: RedisCommands[IO, String, Long] => IO[Unit]): Unit = program(client).unsafeRunSync()

  def tearDown(): Unit = finalizer.unsafeRunSync()
}

private[benchmarks] object Redis4Cats {
  implicit val runtime: IORuntime = IORuntime.global

  def unsafeMake(): Redis4Cats = {
    val resource            = Redis[IO].withOptions(RedisUri, ClientOptions.create(), LongCodec)
    val (client, finalizer) = extractResource(resource).unsafeRunSync()

    new Redis4Cats(client, finalizer)
  }

  private def extractResource[A](resource: Resource[IO, A]): IO[(A, IO[Unit])] =
    for {
      da <- Deferred[IO, A]
      f  <- resource.use(da.complete(_) >> IO.never).start
      a  <- da.get
    } yield a -> f.cancel

  private final val LongCodec = Codecs.derive(RedisCodec.Utf8, stringLongEpi)
  private final val RedisUri  = "redis://127.0.0.1:6379"
}
