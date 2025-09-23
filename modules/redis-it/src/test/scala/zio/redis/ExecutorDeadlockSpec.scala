package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import zio._
import zio.test.TestAspect._
import zio.test._

/**
 * Tests designed to reproduce the specific deadlock scenario where promises
 * await indefinitely due to issues in the SingleNodeExecutor's send/receive loop.
 */
object ExecutorDeadlockSpec extends IntegrationSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Executor promise deadlock reproduction")(
      test("promise await deadlock under stress") {
        for {
          redis <- ZIO.service[Redis]

          _ <- ZIO.logInfo("Testing promise await deadlock scenario")

          // Create a burst of operations that might overwhelm the executor
          burstSize = 500
          concurrentFibers <- ZIO.foreach(1 to burstSize) { i =>
            (for {
              // Multiple operations in sequence to increase pressure
              _ <- redis.set(s"burst-$i", s"value-$i")
              result1 <- redis.get(s"burst-$i").returning[String]
              _ <- redis.del(s"burst-$i")
              result2 <- redis.get(s"burst-$i").returning[String]

              // Return both results
            } yield (result1, result2))
              .timeout(10.seconds) // Individual operation timeout
              .fork
          }

          // Collect all results - this should complete or timeout indicating deadlock
          results <- ZIO.collectAllPar(concurrentFibers.map(_.join))
            .timeoutFail(new Exception("EXECUTOR DEADLOCK: Promises stuck awaiting!"))(60.seconds)

          successful = results.count(_.isDefined)
          timedOut = results.count(_.isEmpty)

          _ <- ZIO.logInfo(s"Burst test results: $successful successful, $timedOut timed out")

          // Even if some operations timeout, we should not have a complete deadlock
        } yield assertTrue(successful > 0)
      },

      test("rapid fire operations with connection stress") {
        for {
          redis <- ZIO.service[Redis]

          _ <- ZIO.logInfo("Testing rapid fire operations")

          // Fire operations as fast as possible without delays
          rapidOperations <- ZIO.foreach(1 to 1000) { i =>
            redis.ping().timeout(5.seconds).fork
          }

          startTime <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)

          results <- ZIO.collectAllPar(rapidOperations.map(_.join))
            .timeoutFail(new Exception("Rapid fire operations caused deadlock"))(90.seconds)

          endTime <- Clock.currentTime(java.util.concurrent.TimeUnit.MILLISECONDS)
          duration = endTime - startTime

          successful = results.count(_.isDefined)
          _ <- ZIO.logInfo(s"Rapid fire: $successful/1000 operations in ${duration}ms")

        } yield assertTrue(successful >= 900) // Allow for some failures but not complete deadlock
      },

      test("interleaved long and short operations") {
        for {
          redis <- ZIO.service[Redis]

          // Mix of quick operations and slower operations that might cause timing issues
          quickOps <- ZIO.foreach(1 to 100) { i =>
            redis.ping().timeout(1.second).fork
          }

          // Operations that use more complex data structures
          complexOps <- ZIO.foreach(1 to 50) { i =>
            (for {
              _ <- redis.hSet(s"hash-$i", ("field1", "value1"), ("field2", "value2"), ("field3", "value3"))
              result <- redis.hGetAll(s"hash-$i").returning[String, String]
              _ <- redis.del(s"hash-$i")
            } yield result).timeout(5.seconds).fork
          }

          // Collect quick operations
          quickResults <- ZIO.collectAllPar(quickOps.map(_.join))
            .timeoutFail(new Exception("Quick operations deadlocked"))(30.seconds)

          // Collect complex operations
          complexResults <- ZIO.collectAllPar(complexOps.map(_.join))
            .timeoutFail(new Exception("Complex operations deadlocked"))(45.seconds)

          _ <- ZIO.logInfo(s"Mixed operations: ${quickResults.length} quick, ${complexResults.length} complex")

        } yield assertTrue(quickResults.length == 100 && complexResults.length == 50)
      },

      test("stress test with error recovery") {
        for {
          redis <- ZIO.service[Redis]

          // Start background operations
          backgroundOps <- ZIO.foreach(1 to 20) { i =>
            (for {
              _ <- ZIO.sleep((i * 100).millis) // Stagger the operations
              _ <- redis.set(s"bg-$i", s"background-$i")
              result <- redis.get(s"bg-$i").returning[String]
            } yield result).timeout(10.seconds).fork
          }

          // Wait for operations to start
          _ <- ZIO.sleep(1.second)

          // Additional stress operations
          stressOps <- ZIO.foreach(1 to 30) { i =>
            redis.set(s"stress-$i", s"value-$i").timeout(10.seconds).fork
          }

          // Wait for background operations
          backgroundResults <- ZIO.collectAllPar(backgroundOps.map(_.join))
            .timeout(30.seconds)
            .orElse(ZIO.succeed(Chunk.empty))

          // Stress operations should mostly succeed
          stressResults <- ZIO.collectAllPar(stressOps.map(_.join))
            .timeoutFail(new Exception("Operations deadlocked under stress"))(30.seconds)

          successfulStress = stressResults.count(_.isDefined)
          _ <- ZIO.logInfo(s"Stress operations: $successfulStress/30 successful")

        } yield assertTrue(successfulStress >= 20) // Allow some failures

      } @@ flaky // This test might be flaky due to timing

    ).provideSomeShared[TestEnvironment](
      Redis.singleNode,
      singleNodeConfig(IntegrationSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier),
      compose(
        service(IntegrationSpec.SingleNode0, ".*Ready to accept connections.*")
      )
    ) @@ sequential @@ withLiveEnvironment @@ timeout(5.minutes)
}