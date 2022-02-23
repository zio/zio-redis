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

package zio.redis.lists

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import zio.ZIO
import zio.duration._
import zio.redis.{BenchmarkRuntime, brPop, rPush}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class BrPopBenchmarks extends BenchmarkRuntime {
  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key = "test-list"

  @Setup(Level.Invocation)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(rPush(key, items.head, items.tail: _*).unit)
  }

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{all => cmd, _}
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(_ => c.send(cmd.blocking.brpop[String](Key.unsafeFrom(key), PosInt.unsafeFrom(1))))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._

    unsafeRun[RediculousClient](c => items.traverse_(_ => RedisCommands.brpop[RedisIO](List(key), 1).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.data._
    import cats.instances.list._
    import cats.syntax.foldable._
    import scala.concurrent.duration._

    unsafeRun[Redis4CatsClient[String]](c =>
      items.traverse_(_ => c.brPop(Duration(1, TimeUnit.SECONDS), NonEmptyList.one(key)))
    )
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(_ => brPop(key)(1.second).returning[String]))
}
