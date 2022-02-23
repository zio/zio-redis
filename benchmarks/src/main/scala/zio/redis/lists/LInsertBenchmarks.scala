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
import zio.redis._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class LInsertBenchmarks extends Benchmark {
  @Param(Array("500"))
  var count: Int = _

  private var items: List[String] = _

  private val key = "test-list"

  @Setup(Level.Invocation)
  def setup(): Unit = {
    items = (0 to count).toList.map(_.toString)
    zioUnsafeRun(rPush(key, items.head, items.tail: _*).unit)
  }

  @TearDown(Level.Invocation)
  def tearDown(): Unit =
    zioUnsafeRun(del(key).unit)

  @Benchmark
  def laserdisc(): Unit = {
    import _root_.laserdisc.fs2._
    import _root_.laserdisc.{all => cmd, _}
    import _root_.laserdisc.lists.listtypes._
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[LaserDiscClient](c =>
      items.traverse_(i => c.send(cmd.linsert[String](Key.unsafeFrom(key), ListPosition.before, i, i)))
    )
  }

  @Benchmark
  def rediculous(): Unit = {
    import cats.implicits._
    import io.chrisdavenport.rediculous._

    unsafeRun[RediculousClient](c => items.traverse_(i => RedisCommands.linsertbefore[RedisIO](key, i, i).run(c)))
  }

  @Benchmark
  def redis4cats(): Unit = {
    import cats.instances.list._
    import cats.syntax.foldable._

    unsafeRun[Redis4CatsClient[String]](c => items.traverse_(i => c.lInsertBefore(key, i, i)))
  }

  @Benchmark
  def zio(): Unit = zioUnsafeRun(ZIO.foreach_(items)(i => lInsert[String, String](key, Position.Before, i, i)))
}
