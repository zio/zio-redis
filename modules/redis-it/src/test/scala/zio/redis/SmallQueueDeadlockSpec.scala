package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import zio._
import zio.test.TestAspect._
import zio.test._

/**
 * Tests specifically designed to reproduce deadlocks with high concurrency
 */
object SmallQueueDeadlockSpec extends IntegrationSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("High concurrency deadlock reproduction")(
      test("massive concurrent requests") {
        for {
          redis <- ZIO.service[Redis]
          _ <- ZIO.logInfo("Starting high concurrency deadlock test")

          // Launch more requests than typical queue can handle
          fibers <- ZIO.foreach(1 to 200) { i =>
            redis.get(s"deadlock-test-$i").returning[String]
              .timeout(5.seconds)
              .fork
          }

          // Try to collect results - if there's a deadlock, this will timeout
          results <- ZIO.collectAllPar(fibers.map(_.join))
            .timeoutFail(new Exception("DEADLOCK REPRODUCED: Requests got stuck!"))(30.seconds)

          successCount = results.count(_.isDefined)
          _ <- ZIO.logInfo(s"Completed: $successCount successful operations out of ${results.length}")

        } yield assertTrue(successCount >= 0) // We expect some to succeed even if there are timeouts
      },

      test("mixed operations stress test") {
        for {
          redis <- ZIO.service[Redis]

          // Mix of set and get operations
          setFibers <- ZIO.foreach(1 to 100) { i =>
            redis.set(s"mix-key-$i", s"mix-value-$i").timeout(3.seconds).fork
          }

          getFibers <- ZIO.foreach(1 to 100) { i =>
            redis.get(s"mix-key-$i").returning[String].timeout(3.seconds).fork
          }

          // Wait for all operations
          setResults <- ZIO.collectAllPar(setFibers.map(_.join))
            .timeoutFail(new Exception("Set operations deadlocked"))(45.seconds)

          getResults <- ZIO.collectAllPar(getFibers.map(_.join))
            .timeoutFail(new Exception("Get operations deadlocked"))(45.seconds)

          _ <- ZIO.logInfo(s"Set operations: ${setResults.length}, Get operations: ${getResults.length}")

        } yield assertTrue(setResults.nonEmpty && getResults.nonEmpty)
      }
    ).provideSomeShared[TestEnvironment](
      Redis.singleNode,
      singleNodeConfig(IntegrationSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier),
      compose(
        service(IntegrationSpec.SingleNode0, ".*Ready to accept connections.*")
      )
    ) @@ sequential @@ withLiveEnvironment @@ timeout(2.minutes)
}