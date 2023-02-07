package zio.redis

import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Promise, Ref, ZIO}

import scala.util.Random

trait PubSubSpec extends BaseSpec {
  def pubSubSuite: Spec[Redis, RedisError] =
    suite("pubSubs")(
      suite("subscribe")(
        test("subscribe response") {
          for {
            redis   <- ZIO.service[Redis]
            channel <- generateRandomString()
            promise <- Promise.make[RedisError, String]
            ref     <- Ref.make(promise)
            resBuilder =
              redis.subscribeWithCallback(channel)((key: String, _: Long) => ref.get.flatMap(_.succeed(key)).unit)
            stream <- resBuilder.returning[String]
            _      <- stream.interruptWhen(promise).runDrain.fork
            res    <- ref.get.flatMap(_.await)
          } yield assertTrue(res == channel)
        },
        test("message response") {
          for {
            redis   <- ZIO.service[Redis]
            channel <- generateRandomString()
            message  = "bar"
            promise <- Promise.make[RedisError, String]
            stream  <- redis.subscribe(channel).returning[String]
            fiber   <- stream.interruptWhen(promise).runHead.fork
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
            redis    <- ZIO.service[Redis]
            prefix   <- generateRandomString(5)
            channel1 <- generateRandomString(prefix)
            channel2 <- generateRandomString(prefix)
            pattern   = prefix + '*'
            message  <- generateRandomString(5)
            stream1 <- redis
                         .subscribe(channel1)
                         .returning[String]
                         .fork
            stream2 <- redis
                         .subscribe(channel2)
                         .returning[String]
                         .fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(channels => channels.size >= 2)
            ch1SubsCount <- redis.publish(channel1, message).replicateZIO(numOfPublish).map(_.head)
            ch2SubsCount <- redis.publish(channel2, message).replicateZIO(numOfPublish).map(_.head)
            promises     <- redis.unsubscribe(List.empty)
            _            <- ZIO.foreachDiscard(promises)(_.await)
            _            <- stream1.join
            _            <- stream2.join
          } yield assertTrue(ch1SubsCount == 1L) && assertTrue(ch2SubsCount == 1L)
        },
        test("psubscribe response") {
          for {
            redis   <- ZIO.service[Redis]
            pattern <- generateRandomString()
            promise <- Promise.make[RedisError, String]
            _ <- redis
                   .pSubscribeWithCallback(pattern)((key: String, _: Long) => promise.succeed(key).unit)
                   .returning[String]
                   .flatMap(_.interruptWhen(promise).runHead)
                   .fork
            res <- promise.await
          } yield assertTrue(res == pattern)
        },
        test("pmessage response") {
          for {
            redis   <- ZIO.service[Redis]
            prefix  <- generateRandomString(5)
            pattern  = prefix + '*'
            channel <- generateRandomString(prefix)
            message <- generateRandomString(prefix)
            stream <- redis
                        .pSubscribe(pattern)
                        .returning[String]
                        .flatMap(_.runHead)
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
            redis   <- ZIO.service[Redis]
            channel <- generateRandomString()
            stream <- redis
                        .subscribe(channel)
                        .returning[Long]
                        .flatMap(_.runFoldWhile(0L)(_ < 10L) { case (sum, message) =>
                          sum + message
                        }.fork)
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
            redis   <- ZIO.service[Redis]
            prefix  <- generateRandomString(5)
            pattern  = prefix + '*'
            channel <- generateRandomString(prefix)
            message <- generateRandomString()
            _ <- redis
                   .subscribe(channel)
                   .returning[String]
                   .flatMap(_.runCollect)
                   .fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(_ contains channel)
            promise       <- redis.unsubscribe(channel)
            _             <- promise.await
            receiverCount <- redis.publish(channel, message).replicateZIO(numOfPublished).map(_.head)
          } yield assertTrue(receiverCount == 0L)
        },
        test("unsubscribe with empty param") {
          for {
            redis    <- ZIO.service[Redis]
            prefix   <- generateRandomString(5)
            pattern   = prefix + '*'
            channel1 <- generateRandomString(prefix)
            channel2 <- generateRandomString(prefix)
            _ <-
              redis
                .subscribe(channel1)
                .returning[String]
                .flatMap(_.runCollect)
                .fork
            _ <-
              redis
                .subscribe(channel2)
                .returning[String]
                .flatMap(_.runCollect)
                .fork
            _ <- redis
                   .pubSubChannels(pattern)
                   .repeatUntil(_.size >= 2)
            _               <- redis.unsubscribe(List.empty).flatMap(ZIO.foreach(_)(_.await))
            numSubResponses <- redis.pubSubNumSub(channel1, channel2)
          } yield assertTrue(
            numSubResponses == Chunk(
              NumSubResponse(channel1, 0L),
              NumSubResponse(channel2, 0L)
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
