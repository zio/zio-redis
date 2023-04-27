package zio.redis

import zio.redis.options.PubSub.NumberOfSubscribers
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Promise, ZIO}

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
            resBuilder =
              subscription
                .subscribeWithCallback(channel)(
                  (key: String, _: Long) => promise.succeed(key).unit,
                  (_, _) => ZIO.unit
                )
            stream = resBuilder.returning[String]
            _ <- stream
                   .interruptWhen(promise)
                   .runDrain
                   .fork
            res <- promise.await
          } yield assertTrue(res == channel)
        },
        test("message response") {
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            message       = "bar"
            promise      <- Promise.make[RedisError, String]
            stream        = subscription.subscribe(channel).returning[String]
            fiber        <- stream.interruptWhen(promise).runHead.fork
            _ <- redis
                   .pubSubChannels(channel)
                   .repeatUntil(_ contains channel)
            _   <- redis.publish(channel, message)
            res <- fiber.join
          } yield assertTrue(res.get == message)
        },
        test("multiple subscribe") {
          val numOfPublish = 20
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            prefix       <- generateRandomString(5)
            channel1     <- generateRandomString(prefix)
            channel2     <- generateRandomString(prefix)
            pattern       = prefix + '*'
            message      <- generateRandomString(5)
            stream1 = subscription
                        .subscribe(channel1)
                        .returning[String]
            stream2 = subscription
                        .subscribe(channel2)
                        .returning[String]
            fiber1 <- stream1.runDrain.fork
            fiber2 <- stream2.runDrain.fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(channels => channels.size >= 2)
            ch1SubsCount <- redis.publish(channel1, message).replicateZIO(numOfPublish).map(_.head)
            ch2SubsCount <- redis.publish(channel2, message).replicateZIO(numOfPublish).map(_.head)
            _            <- subscription.unsubscribe()
            _            <- fiber1.join
            _            <- fiber2.join
          } yield assertTrue(ch1SubsCount == 1L) && assertTrue(ch2SubsCount == 1L)
        },
        test("psubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            pattern      <- generateRandomString()
            promise      <- Promise.make[RedisError, String]
            _ <- subscription
                   .pSubscribeWithCallback(pattern)(
                     (key: String, _: Long) => promise.succeed(key).unit,
                     (_, _) => ZIO.unit
                   )
                   .returning[String]
                   .interruptWhen(promise)
                   .runHead
                   .fork
            res <- promise.await
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
            stream <- subscription
                        .pSubscribe(pattern)
                        .returning[String]
                        .runHead
                        .fork
            _   <- redis.pubSubNumPat.repeatUntil(_ > 0)
            _   <- redis.publish(channel, message)
            res <- stream.join
          } yield assertTrue(res.get == message)
        }
      ),
      suite("publish")(test("publish long type message") {
        val message = 1L
        assertZIO(
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            stream <- subscription
                        .subscribe(channel)
                        .returning[Long]
                        .runFoldWhile(0L)(_ < 10L) { case (sum, message) =>
                          sum + message
                        }
                        .fork
            _   <- redis.pubSubChannels(channel).repeatUntil(_ contains channel)
            _   <- ZIO.replicateZIO(10)(redis.publish(channel, message))
            res <- stream.join
          } yield res
        )(equalTo(10L))
      }),
      suite("unsubscribe")(
        test("don't receive message type after unsubscribe") {
          val numOfPublished = 5
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            prefix       <- generateRandomString(5)
            pattern       = prefix + '*'
            channel      <- generateRandomString(prefix)
            message      <- generateRandomString()
            promise      <- Promise.make[Nothing, Unit]
            _ <- subscription
                   .subscribeWithCallback(channel)((_, _) => ZIO.unit, (_, _) => promise.succeed(()).unit)
                   .returning[String]
                   .runCollect
                   .fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(_ contains channel)
            _             <- subscription.unsubscribe(channel)
            _             <- promise.await
            receiverCount <- redis.publish(channel, message).replicateZIO(numOfPublished).map(_.head)
          } yield assertTrue(receiverCount == 0L)
        },
        test("unsubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            channel      <- generateRandomString()
            promise      <- Promise.make[RedisError, String]
            _ <- subscription
                   .subscribeWithCallback(channel)(
                     (_, _) => ZIO.unit,
                     (key, _) => promise.succeed(key).unit
                   )
                   .returning[Unit]
                   .runDrain
                   .fork
            _   <- subscription.unsubscribe(channel)
            res <- promise.await
          } yield assertTrue(res == channel)
        },
        test("punsubscribe response") {
          for {
            subscription <- ZIO.service[RedisSubscription]
            pattern      <- generateRandomString()
            promise      <- Promise.make[RedisError, String]
            _ <- subscription
                   .pSubscribeWithCallback(pattern)(
                     (_, _) => ZIO.unit,
                     (key, _) => promise.succeed(key).unit
                   )
                   .returning[Unit]
                   .runDrain
                   .fork
            _   <- subscription.pUnsubscribe(pattern)
            res <- promise.await
          } yield assertTrue(res == pattern)
        },
        test("unsubscribe with empty param") {
          for {
            redis        <- ZIO.service[Redis]
            subscription <- ZIO.service[RedisSubscription]
            prefix       <- generateRandomString(5)
            pattern       = prefix + '*'
            channel1     <- generateRandomString(prefix)
            channel2     <- generateRandomString(prefix)
            promise1     <- Promise.make[Nothing, Unit]
            promise2     <- Promise.make[Nothing, Unit]
            _ <-
              subscription
                .subscribeWithCallback(channel1)(
                  (_, _) => ZIO.unit,
                  (_, _) => promise1.succeed(()).unit
                )
                .returning[String]
                .runCollect
                .fork
            _ <-
              subscription
                .subscribeWithCallback(channel2)(
                  (_, _) => ZIO.unit,
                  (_, _) => promise2.succeed(()).unit
                )
                .returning[String]
                .runCollect
                .fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(_.size >= 2)
            _               <- subscription.unsubscribe()
            _               <- promise1.await
            _               <- promise2.await
            numSubResponses <- redis.pubSubNumSub(channel1, channel2)
          } yield assertTrue(
            numSubResponses == Chunk(
              NumberOfSubscribers(channel1, 0L),
              NumberOfSubscribers(channel2, 0L)
            )
          )
        }
      )
    )

  private def generateRandomString(prefix: String = "") =
    ZIO.succeed(Random.alphanumeric.take(15).mkString).map(prefix + _.substring((prefix.length - 1) max 0))

  private def generateRandomString(len: Int) =
    ZIO.succeed(Random.alphanumeric.take(len).mkString)
}
