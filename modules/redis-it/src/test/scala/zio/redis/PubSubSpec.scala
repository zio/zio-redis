package zio.redis

import zio.test.Assertion._
import zio.test._
import zio.{Promise, ZIO}

import scala.util.Random

trait PubSubSpec extends BaseSpec {
  def pubSubSuite: Spec[Redis with RedisSubscription, RedisError] =
    suite("pubSubs")(
      suite("subscribe")(
        test("subscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            promise      <- Promise.make[RedisError, String]
            resBuilder    =
              subscription
                .subscribeSingleWith(channel)(onSubscribe = (key: String, _: Long) => promise.succeed(key).unit)
            stream        = resBuilder.returning[String]
            _            <- stream
                              .interruptWhen(promise)
                              .runDrain
                              .fork
            res          <- promise.await
          } yield assertTrue(res == channel)
        },
        test("message response") {
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            message       = "bar"
            promise      <- Promise.make[RedisError, Unit]
            fiber        <- subscription
                              .subscribeSingleWith(channel)(
                                onSubscribe = (_, _) => promise.succeed(()).unit
                              )
                              .returning[String]
                              .runHead
                              .fork
            _            <- promise.await
            _            <- redis.publish(channel, message)
            res          <- fiber.join
          } yield assertTrue(res.get == message)
        },
        test("multiple subscribe") {
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            prefix       <- generateRandomString(5)
            channel1     <- generateRandomString(prefix)
            channel2     <- generateRandomString(prefix)
            message      <- generateRandomString(5)
            subsPromise1 <- Promise.make[RedisError, Unit]
            subsPromise2 <- Promise.make[RedisError, Unit]
            stream1       = subscription
                              .subscribeSingleWith(channel1)(
                                onSubscribe = (_, _) => subsPromise1.succeed(()).unit
                              )
                              .returning[String]
            stream2       = subscription
                              .subscribeSingleWith(channel2)(
                                onSubscribe = (_, _) => subsPromise2.succeed(()).unit
                              )
                              .returning[String]
            fiber1       <- stream1.runDrain.fork
            fiber2       <- stream2.runDrain.fork
            _            <- subsPromise1.await *> subsPromise2.await
            ch1SubsCount <- redis.publish(channel1, message)
            ch2SubsCount <- redis.publish(channel2, message)
            _            <- subscription.unsubscribe()
            _            <- fiber1.join
            _            <- fiber2.join
          } yield assertTrue(ch1SubsCount == 1L, ch2SubsCount == 1L)
        },
        test("psubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            pattern      <- generateRandomString()
            promise      <- Promise.make[RedisError, String]
            _            <- subscription
                              .pSubscribeWith(pattern)(onSubscribe = (key: String, _: Long) => promise.succeed(key).unit)
                              .returning[String]
                              .runHead
                              .fork
            res          <- promise.await
          } yield assertTrue(res == pattern)
        },
        test("pmessage response") {
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            prefix       <- generateRandomString(5)
            pattern       = prefix + '*'
            channel      <- generateRandomString(prefix)
            message      <- generateRandomString(prefix)
            promise      <- Promise.make[RedisError, Unit]
            stream       <- subscription
                              .pSubscribeWith(pattern)(
                                onSubscribe = (_, _) => promise.succeed(()).unit
                              )
                              .returning[String]
                              .runHead
                              .fork
            _            <- promise.await
            _            <- redis.publish(channel, message)
            res          <- stream.join
          } yield assertTrue(res.map(_._2).get == message)
        }
      ),
      suite("publish")(test("publish long type message") {
        val message = 1L
        assertZIO(
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            promise      <- Promise.make[RedisError, Unit]
            stream       <- subscription
                              .subscribeSingleWith(channel)(
                                onSubscribe = (_, _) => promise.succeed(()).unit
                              )
                              .returning[Long]
                              .runFoldWhile(0L)(_ < 10L) { case (sum, message) =>
                                sum + message
                              }
                              .fork
            _            <- promise.await
            _            <- ZIO.replicateZIO(10)(redis.publish(channel, message))
            res          <- stream.join
          } yield res
        )(equalTo(10L))
      }),
      suite("unsubscribe")(
        test("don't receive message type after unsubscribe") {
          for {
            redis         <- ZIO.service[Redis]
            subscription  <- ZIO.service[RedisSubscription]
            prefix        <- generateRandomString(5)
            channel       <- generateRandomString(prefix)
            subsPromise   <- Promise.make[Nothing, Unit]
            promise       <- Promise.make[Nothing, Unit]
            _             <- subscription
                               .subscribeSingleWith(channel)(
                                 onSubscribe = (_, _) => subsPromise.succeed(()).unit,
                                 onUnsubscribe = (_, _) => promise.succeed(()).unit
                               )
                               .returning[String]
                               .runDrain
                               .fork
            _             <- subsPromise.await
            _             <- subscription.unsubscribe(channel)
            _             <- promise.await
            receiverCount <- redis.pubSubNumSub(channel).map(_.getOrElse(channel, 0L))
          } yield assertTrue(receiverCount == 0L)
        },
        test("unsubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            subsPromise  <- Promise.make[RedisError, Unit]
            promise      <- Promise.make[RedisError, String]
            fiber        <- subscription
                              .subscribeSingleWith(channel)(
                                onSubscribe = (_, _) => subsPromise.succeed(()).unit,
                                onUnsubscribe = (key, _) => promise.succeed(key).unit
                              )
                              .returning[Unit]
                              .runDrain
                              .fork
            _            <- subsPromise.await
            _            <- subscription.unsubscribe(channel)
            _            <- fiber.join
            res          <- promise.await
          } yield assertTrue(res == channel)
        },
        test("punsubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            pattern      <- generateRandomString()
            subsPromise  <- Promise.make[RedisError, Unit]
            promise      <- Promise.make[RedisError, String]
            fiber        <- subscription
                              .pSubscribeWith(pattern)(
                                onSubscribe = (_, _) => subsPromise.succeed(()).unit,
                                onUnsubscribe = (key, _) => promise.succeed(key).unit
                              )
                              .returning[Unit]
                              .runDrain
                              .fork
            _            <- subsPromise.await
            _            <- subscription.pUnsubscribe(pattern)
            _            <- fiber.join
            res          <- promise.await
          } yield assertTrue(res == pattern)
        },
        test("unsubscribe with empty param") {
          for {
            redis           <- ZIO.service[Redis]
            subscription    <- ZIO.service[RedisSubscription]
            prefix          <- generateRandomString(5)
            channel1        <- generateRandomString(prefix)
            channel2        <- generateRandomString(prefix)
            subsPromise1    <- Promise.make[Nothing, Unit]
            subsPromise2    <- Promise.make[Nothing, Unit]
            promise1        <- Promise.make[Nothing, Unit]
            promise2        <- Promise.make[Nothing, Unit]
            _               <-
              subscription
                .subscribeSingleWith(channel1)(
                  onSubscribe = (_, _) => subsPromise1.succeed(()).unit,
                  onUnsubscribe = (_, _) => promise1.succeed(()).unit
                )
                .returning[String]
                .runCollect
                .fork
            _               <-
              subscription
                .subscribeSingleWith(channel2)(
                  onSubscribe = (_, _) => subsPromise2.succeed(()).unit,
                  onUnsubscribe = (_, _) => promise2.succeed(()).unit
                )
                .returning[String]
                .runCollect
                .fork
            _               <- subsPromise1.await *> subsPromise2.await
            _               <- subscription.unsubscribe()
            _               <- promise1.await *> promise2.await
            numSubResponses <- redis.pubSubNumSub(channel1, channel2)
          } yield assertTrue(
            numSubResponses == Map(
              channel1 -> 0L,
              channel2 -> 0L
            )
          )
        }
      )
    ) @@ TestAspect.eventually

  private def generateRandomString(prefix: String = "") =
    ZIO.succeed(Random.alphanumeric.take(15).mkString).map(prefix + _.substring((prefix.length - 1) max 0))

  private def generateRandomString(len: Int) =
    ZIO.succeed(Random.alphanumeric.take(len).mkString)
}
