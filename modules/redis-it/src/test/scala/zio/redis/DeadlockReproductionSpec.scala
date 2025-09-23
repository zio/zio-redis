package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import zio._
import zio.test.TestAspect._
import zio.test._

import java.util.concurrent.TimeUnit

object DeadlockReproductionSpec extends IntegrationSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Redis deadlock reproduction tests")(
      suite("Queue backpressure deadlock")(
        test("concurrent requests exceed queue capacity") {
          for {
            redis <- ZIO.service[Redis]
            _ <- ZIO.logInfo("Starting queue backpressure test")

            // Flood the queue with concurrent requests
            fibers <- ZIO.foreach(1 to 1000) { i =>
              redis.get(s"key-$i").returning[String].fork
            }

            // Try to join all fibers - this should deadlock if the issue exists
            results <- ZIO.collectAllPar(fibers.map(_.join))
              .timeoutFail(new Exception("Deadlock detected: requests timed out"))(30.seconds)

            _ <- ZIO.logInfo(s"Completed ${results.length} requests")
          } yield assertTrue(results.length == 1000)
        },

        test("mixed read/write operations under backpressure") {
          for {
            redis <- ZIO.service[Redis]

            // Create rapid concurrent mixed operations
            writeFibers <- ZIO.foreach(1 to 500) { i =>
              redis.set(s"key-$i", s"value-$i").fork
            }

            readFibers <- ZIO.foreach(1 to 500) { i =>
              redis.get(s"key-$i").returning[String].fork
            }

            // Join all operations with timeout
            _ <- ZIO.collectAllPar(writeFibers.map(_.join))
              .timeoutFail(new Exception("Write operations deadlocked"))(60.seconds)

            _ <- ZIO.collectAllPar(readFibers.map(_.join))
              .timeoutFail(new Exception("Read operations deadlocked"))(60.seconds)

          } yield assertTrue(true)
        }
      ),

      suite("Connection failure scenarios")(
        test("simulated connection stress") {
          for {
            redis <- ZIO.service[Redis]

            // Start some background operations
            backgroundFibers <- ZIO.foreach(1 to 100) { i =>
              (for {
                _ <- ZIO.sleep(100.millis)
                _ <- redis.set(s"bg-key-$i", s"bg-value-$i")
                result <- redis.get(s"bg-key-$i").returning[String]
              } yield result).timeout(5.seconds).forever.fork
            }

            // Wait a bit for operations to start
            _ <- ZIO.sleep(2.seconds)

            // Try new operations - these should not deadlock
            newOperations <- ZIO.foreach(1 to 50) { i =>
              redis.get(s"test-key-$i").returning[String].timeout(10.seconds).fork
            }

            results <- ZIO.collectAllPar(newOperations.map(_.join))
              .timeoutFail(new Exception("Operations deadlocked under stress"))(30.seconds)

            // Clean up background fibers
            _ <- ZIO.foreachDiscard(backgroundFibers)(_.interrupt)

          } yield assertTrue(results.length == 50)
        } @@ flaky // This test might be flaky due to timing
      ),

      suite("High concurrency stress test")(
        test("massive concurrent get operations") {
          for {
            redis <- ZIO.service[Redis]

            // Pre-populate some keys
            _ <- ZIO.foreachDiscard(1 to 100) { i =>
              redis.set(s"stress-key-$i", s"stress-value-$i")
            }

            // Launch massive number of concurrent get operations
            startTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
            fibers <- ZIO.foreach(1 to 2000) { i =>
              redis.get(s"stress-key-${i % 100}").returning[String].fork
            }

            // Collect results with timeout to detect deadlocks
            results <- ZIO.collectAllPar(fibers.map(_.join))
              .timeoutFail(new Exception("Massive concurrency caused deadlock"))(120.seconds)

            endTime <- Clock.currentTime(TimeUnit.MILLISECONDS)
            duration = endTime - startTime

            _ <- ZIO.logInfo(s"Completed ${results.length} operations in ${duration}ms")

          } yield assertTrue(results.length == 2000)
        },

        test("interleaved operations with fiber racing") {
          for {
            redis <- ZIO.service[Redis]

            // Create racing operations on the same keys
            racingOperations <- ZIO.foreach(1 to 100) { i =>
              val key = s"race-key-${i % 10}" // Intentional key collision

              ZIO.raceAll(
                redis.set(key, s"value-$i"),
                List(
                  redis.get(key).returning[String].map(_.getOrElse("not-found")),
                  redis.del(key),
                  redis.set(key, s"updated-value-$i")
                )
              ).fork
            }

            // Wait for all racing operations to complete
            results <- ZIO.collectAllPar(racingOperations.map(_.join))
              .timeoutFail(new Exception("Racing operations caused deadlock"))(60.seconds)

          } yield assertTrue(results.length == 100)
        }
      ),

      suite("Resource cleanup and error handling")(
        test("proper cleanup on fiber interruption") {
          for {
            redis <- ZIO.service[Redis]

            // Start long-running operations that will be interrupted
            longRunningFibers <- ZIO.foreach(1 to 50) { i =>
              (for {
                _ <- ZIO.sleep(10.seconds) // Long delay
                _ <- redis.get(s"cleanup-key-$i").returning[String]
              } yield ()).fork
            }

            // Let them start, then interrupt them
            _ <- ZIO.sleep(1.second)
            _ <- ZIO.foreachDiscard(longRunningFibers)(_.interrupt)

            // Verify that new operations can still proceed after interruptions
            newOperations <- ZIO.foreach(1 to 20) { i =>
              redis.set(s"after-interrupt-$i", s"value-$i").fork
            }

            results <- ZIO.collectAllPar(newOperations.map(_.join))
              .timeoutFail(new Exception("Operations failed after fiber interruption"))(30.seconds)

          } yield assertTrue(results.length == 20)
        }
      )
    ).provideSomeShared[TestEnvironment](
      Redis.singleNode,
      singleNodeConfig(IntegrationSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier),
      compose(
        service(IntegrationSpec.SingleNode0, ".*Ready to accept connections.*")
      )
    ) @@ sequential @@ withLiveEnvironment
}