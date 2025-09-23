package zio.redis

import zio._
import zio.test.TestAspect._
import zio.test._

/**
 * Simple test to validate diagnostic logging is working
 */
object DiagnosticLoggingSpec extends IntegrationSpec {

  def spec: Spec[TestEnvironment, Any] =
    suite("Diagnostic logging validation")(
      test("basic redis operations with diagnostic logging") {
        for {
          redis <- ZIO.service[Redis]
          _     <- ZIO.logInfo("Starting diagnostic logging test")

          // Simple get/set operations to trigger logging
          _      <- redis.set("diagnostic-key", "diagnostic-value")
          result <- redis.get("diagnostic-key").returning[String]

          _ <- ZIO.logInfo(s"Diagnostic test completed, result: $result")
        } yield assertTrue(result == Some("diagnostic-value"))
      },

      test("multiple concurrent operations to trigger detailed logging") {
        for {
          redis <- ZIO.service[Redis]

          // Run multiple operations concurrently to see queue logging
          fibers <- ZIO.foreach(1 to 10) { i =>
            redis.set(s"concurrent-key-$i", s"value-$i").fork
          }

          _ <- ZIO.collectAllPar(fibers.map(_.join))

          // Read them back
          results <- ZIO.foreach(1 to 10) { i =>
            redis.get(s"concurrent-key-$i").returning[String]
          }

        } yield assertTrue(results.flatten.length == 10)
      }
    ).provideSomeShared[TestEnvironment](
      Redis.singleNode,
      singleNodeConfig(IntegrationSpec.SingleNode0),
      ZLayer.succeed(ProtobufCodecSupplier),
      compose(
        service(IntegrationSpec.SingleNode0, ".*Ready to accept connections.*")
      )
    ) @@ sequential @@ withLiveEnvironment
}