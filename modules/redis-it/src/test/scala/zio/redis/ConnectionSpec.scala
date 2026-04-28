package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import org.testcontainers.DockerClientFactory
import zio._
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

trait ConnectionSpec extends IntegrationSpec {
  def connectionSuite: Spec[DockerComposeContainer & Redis, RedisError] =
    suite("connection")(
      suite("authenticating")(
        test("auth with 'default' username") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.auth("default", "password")
          } yield assert(res)(isUnit)
        },
        test("auth required error") {
          for {
            redisError <- ZIO.serviceWithZIO[Redis](_.ping()).flip.orDieWith(new Throwable(_))
          } yield assertTrue(redisError.asInstanceOf[ProtocolError].message == "Authentication required.")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2),
          ZLayer.succeed(ProtobufCodecSupplier)
        ),
        test("automatic auth") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        ),
        test("subscription auth required error") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            redisError   <- subscription
                              .subscribeSingleWith("auth-test-channel")()
                              .returning[String]
                              .runHead
                              .flip
                              .orDieWith(res => new Throwable(s"Expected failure, got $res"))
          } yield assertTrue(redisError.asInstanceOf[ProtocolError].message == "Authentication required.")
        }.provideSome[DockerComposeContainer](
          RedisSubscription.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2),
          ZLayer.succeed(ProtobufCodecSupplier)
        ),
        test("subscription automatic auth") {
          for {
            channel      <- Random.nextUUID.map(uuid => "auth-test-channel-$uuid")
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            subscribed   <- Promise.make[RedisError, Unit]
            fiber        <- subscription
                              .subscribeSingleWith(channel)(
                                onSubscribe = (_, _) => subscribed.succeed(()).unit
                              )
                              .returning[String]
                              .runHead
                              .fork
            _            <- subscribed.await
            _            <- redis.publish(channel, "hello")
            received     <- fiber.join
          } yield assertTrue(received.contains("hello"))
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          RedisSubscription.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        )
      ),
      suite("clientId")(
        test("get client id") {
          for {
            id <- ZIO.serviceWithZIO[Redis](_.clientId)
          } yield assert(id)(isGreaterThan(0L))
        }
      ),
      test("set and get name") {
        for {
          redis <- ZIO.service[Redis]
          _     <- redis.clientSetName("foo")
          name  <- redis.clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      } @@ clusterExecutorUnsupported,
      suite("ping")(
        test("without argument, returns PONG") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        },
        test("with an argument, returns the argument") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping(Some("toto")))
          } yield assertTrue(pong == "toto")
        }
      ),
      suite("reconnect")(
        test("restart redis") {
          for {
            docker     <- ZIO.service[DockerComposeContainer]
            containerId = docker.getContainerByServiceName(IntegrationSpec.SingleNode0).get.getContainerId()
            _           = DockerClientFactory.instance().client().restartContainerCmd(containerId).exec()
            pong       <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        },
        test("restart password protected redis") {
          for {
            docker     <- ZIO.service[DockerComposeContainer]
            containerId = docker.getContainerByServiceName(IntegrationSpec.SingleNode1).get.getContainerId()
            _           = DockerClientFactory.instance().client().restartContainerCmd(containerId).exec()
            pong       <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        )
      ) @@ clusterExecutorUnsupported
    ) @@ sequential
}
