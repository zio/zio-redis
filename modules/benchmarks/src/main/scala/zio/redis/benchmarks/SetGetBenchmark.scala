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

import cats.syntax.parallel._
import org.openjdk.jmh.annotations._
import zio.redis._
import zio.{Scope => _, _}

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class SetGetBenchmark {
  import SetGetBenchmark._

  @Param(Array("20"))
  var parallelism: Int = _

  @Param(Array("500"))
  var repetitions: Int = _

  @Setup
  def setup(): Unit = {
    redis4Cats = Redis4Cats.unsafeMake()
    zioRedis = ZioRedis.unsafeMake()
  }

  @TearDown
  def tearDown(): Unit = {
    redis4Cats.tearDown()
    zioRedis.tearDown()
  }

  @Benchmark
  def redis4cats(): Unit =
    redis4Cats.run { client =>
      val task  = client.set(TestKey, TestValue) >> client.get(TestKey)
      val tasks = List.fill(parallelism)(task.replicateA_(repetitions))

      tasks.parSequence_
    }

  @Benchmark
  def zio(): Unit =
    zioRedis.run {
      for {
        redis <- ZIO.service[Redis]
        task   = redis.set(TestKey, TestValue) *> redis.get(TestKey).returning[Long]
        tasks  = Chunk.fill(parallelism)(task.repeatN(repetitions))
        _     <- ZIO.collectAllParDiscard(tasks)
      } yield ()
    }

  private var redis4Cats: Redis4Cats = _
  private var zioRedis: ZioRedis     = _
}

object SetGetBenchmark {
  private final val TestKey   = "TestKey"
  private final val TestValue = 1024L
}
