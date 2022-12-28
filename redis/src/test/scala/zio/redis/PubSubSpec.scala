package zio.redis

import zio.test.Assertion._
import zio.test._
import zio.{Chunk, ZIO}

import scala.util.Random

trait PubSubSpec extends BaseSpec {
  def pubSubSuite: Spec[Redis, RedisError] =
    suite("pubSubs")(
      suite("subscribe")(
        test("subscribe response") {
          for {
            channel <- generateRandomString()
            res     <- subscribe(channel).runHead
          } yield assertTrue(res.get.key == SubscriptionKey.Channel(channel))
        },
        test("message response") {
          for {
            channel <- generateRandomString()
            message  = "bar"
            stream <- subscribeStreamBuilder(channel)
                        .returning[String]
                        .runHead
                        .fork
            _ <- pubSubChannels(channel)
                   .repeatUntil(_ contains channel)
            _   <- publish(channel, message)
            res <- stream.join
          } yield assertTrue(res.get == message)
        },
        test("multiple subscribe") {
          val numOfPublish = 20
          for {
            prefix   <- generateRandomString(5)
            channel1 <- generateRandomString(prefix)
            channel2 <- generateRandomString(prefix)
            pattern   = prefix + '*'
            message  <- generateRandomString(5)
            stream1 <- subscribe(channel1)
                         .runFoldWhile(Chunk.empty[PushProtocol])(
                           _.forall(_.isInstanceOf[PushProtocol.Unsubscribe] == false)
                         )(_ appended _)
                         .fork
            stream2 <- subscribe(channel2)
                         .runFoldWhile(Chunk.empty[PushProtocol])(
                           _.forall(_.isInstanceOf[PushProtocol.Unsubscribe] == false)
                         )(_ appended _)
                         .fork
            _ <- pubSubChannels(pattern)
                   .repeatUntil(channels => channels.size >= 2)
            ch1SubsCount <- publish(channel1, message).replicateZIO(numOfPublish).map(_.head)
            ch2SubsCount <- publish(channel2, message).replicateZIO(numOfPublish).map(_.head)
            _            <- unsubscribe().runDrain.fork
            res1         <- stream1.join
            res2         <- stream2.join
          } yield assertTrue(ch1SubsCount == 1L) &&
            assertTrue(ch2SubsCount == 1L) &&
            assertTrue(res1.size == numOfPublish + 2) &&
            assertTrue(res2.size == numOfPublish + 2)
        },
        test("psubscribe response") {
          for {
            pattern <- generateRandomString()
            res     <- pSubscribe(pattern).runHead
          } yield assertTrue(res.get.key.value == pattern)
        },
        test("pmessage response") {
          for {
            prefix  <- generateRandomString(5)
            pattern  = prefix + '*'
            channel <- generateRandomString(prefix)
            message <- generateRandomString(prefix)
            stream <- pSubscribeStreamBuilder(pattern)
                        .returning[String]
                        .runHead
                        .fork
            _   <- pubSubNumPat.repeatUntil(_ > 0)
            _   <- publish(channel, message)
            res <- stream.join
          } yield assertTrue(res.get == message)
        }
      ),
      suite("publish")(test("publish long type message") {
        val message = 1L
        assertZIO(
          for {
            channel <- generateRandomString()
            stream <- subscribeStreamBuilder(channel)
                        .returning[Long]
                        .runFoldWhile(0L)(_ < 10L) { case (sum, message) =>
                          sum + message
                        }
                        .fork
            _   <- pubSubChannels(channel).repeatUntil(_ contains channel)
            _   <- ZIO.replicateZIO(10)(publish(channel, message))
            res <- stream.join
          } yield res
        )(equalTo(10L))
      }),
      suite("unsubscribe")(
        test("don't receive message type after unsubscribe") {
          val numOfPublished = 5
          for {
            prefix  <- generateRandomString(5)
            pattern  = prefix + '*'
            channel <- generateRandomString(prefix)
            message <- generateRandomString()
            stream <- subscribe(channel)
                        .runFoldWhile(Chunk.empty[PushProtocol])(_.size < 2)(_ appended _)
                        .fork
            _ <- pubSubChannels(pattern)
                   .repeatUntil(_ contains channel)
            _             <- unsubscribe(channel).runHead
            receiverCount <- publish(channel, message).replicateZIO(numOfPublished).map(_.head)
            res           <- stream.join
          } yield assertTrue(
            res.size == 2
          ) && assertTrue(receiverCount == 0L)
        },
        test("unsubscribe response") {
          for {
            channel <- generateRandomString()
            res     <- unsubscribe(channel).runHead
          } yield assertTrue(res.get.key.value == channel)
        },
        test("punsubscribe response") {
          for {
            pattern <- generateRandomString()
            res     <- pUnsubscribe(pattern).runHead
          } yield assertTrue(res.get.key.value == pattern)
        },
        test("unsubscribe with empty param") {
          for {
            prefix   <- generateRandomString(5)
            pattern   = prefix + '*'
            channel1 <- generateRandomString(prefix)
            channel2 <- generateRandomString(prefix)
            stream1 <-
              subscribe(channel1)
                .runFoldWhile(Chunk.empty[PushProtocol])(_.forall(_.isInstanceOf[PushProtocol.Unsubscribe] == false))(
                  _ appended _
                )
                .fork
            stream2 <-
              subscribe(channel2)
                .runFoldWhile(Chunk.empty[PushProtocol])(_.forall(_.isInstanceOf[PushProtocol.Unsubscribe] == false))(
                  _ appended _
                )
                .fork
            _ <- pubSubChannels(pattern)
                   .repeatUntil(_.size >= 2)
            _                   <- unsubscribe().runDrain.fork
            unsubscribeMessages <- stream1.join zip stream2.join
            (result1, result2)   = unsubscribeMessages
            numSubResponses     <- pubSubNumSub(channel1, channel2)
          } yield assertTrue(
            result1.size == 2 && result2.size == 2
          ) && assertTrue(
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
