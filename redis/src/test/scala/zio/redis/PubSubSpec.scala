package zio.redis

import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Promise, ZIO}

trait PubSubSpec extends BaseSpec {
  def pubSubSuite: Spec[Redis, RedisError] =
    suite("pubSubs")(
      suite("subscribe")(
        test("subscribe response") {
          val channel = "foo"
          assertZIO(subscribe(channel).returning[String].runHead)(
            isSome(
              isSubtype[PushProtocol.Subscribe](equalTo(PushProtocol.Subscribe(channel, 1L)))
            )
          )
        },
        test("message response") {
          val channel = "foo"
          val message = "bar"
          assertZIO(
            for {
              promise <- Promise.make[RedisError, Unit]
              stream <- subscribe(channel)
                          .returning[String]
                          .interruptWhen(promise)
                          .runCollect
                          .fork
              _ <- pubSubChannels(channel)
                     .repeatUntil(_ contains channel)
              _   <- publish(channel, message)
              _   <- promise.succeed(())
              res <- stream.join
            } yield res
          )(
            equalTo(
              Chunk(
                PushProtocol.Subscribe(channel, 1L),
                PushProtocol.Message(channel, message)
              )
            )
          )
        },
        test("multiple subscribe") {
          val channel1     = "bar"
          val channel2     = "baz"
          val message      = "message"
          val numOfPublish = 9
          for {
            promise <- Promise.make[RedisError, Unit]
            stream1 <- subscribe(channel1)
                         .returning[String]
                         .interruptWhen(promise)
                         .runCollect
                         .fork
            stream2 <- subscribe(channel2)
                         .returning[String]
                         .interruptWhen(promise)
                         .runCollect
                         .fork
            _ <- pubSubChannels("ba*")
                   .repeatUntil(channels => channels.size == 2)
            ch1SubsCount <- publish(channel1, message).replicateZIO(numOfPublish).map(_.head)
            ch2SubsCount <- publish(channel2, message).replicateZIO(numOfPublish).map(_.head)
            _            <- promise.succeed(())
            res1         <- stream1.join
            res2         <- stream2.join
          } yield assertTrue(ch1SubsCount == 1L) &&
            assertTrue(ch2SubsCount == 1L) &&
            assertTrue(res1.size == numOfPublish + 1) &&
            assertTrue(res2.size == numOfPublish + 1)
        },
        test("psubscribe response") {
          val pattern = "f*"
          assertZIO(pSubscribe(pattern).returning[String].runHead)(
            isSome(
              isSubtype[PushProtocol.PSubscribe](equalTo(PushProtocol.PSubscribe(pattern, 1L)))
            )
          )
        },
        test("pmessage response") {
          val pattern = "f*"
          val channel = "foo"
          val message = "bar"
          assertZIO(
            for {
              promise <- Promise.make[RedisError, Unit]
              stream <- pSubscribe(pattern)
                          .returning[String]
                          .interruptWhen(promise)
                          .runCollect
                          .fork
              _   <- pubSubNumPat.repeatUntil(_ > 0)
              _   <- publish(channel, message)
              _   <- promise.succeed(())
              res <- stream.join
            } yield res
          )(
            equalTo(
              Chunk(PushProtocol.PSubscribe(pattern, 1L), PushProtocol.PMessage(pattern, channel, message))
            )
          )
        }
      ),
      suite("publish")(test("publish long type message") {
        val channel = "foo"
        val message = 1L
        assertZIO(
          for {
            stream <- subscribe(channel)
                        .returning[Long]
                        .collect { case PushProtocol.Message(_, message) => message }
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
          val channel        = "foo"
          val message        = "bar"
          val numOfPublished = 10
          for {
            promise <- Promise.make[RedisError, Unit]
            stream <- subscribe(channel)
                        .returning[String]
                        .interruptWhen(promise)
                        .runCollect
                        .fork
            _ <- pubSubChannels(channel)
                   .repeatUntil(_ contains channel)
            _             <- unsubscribe(channel).runHead
            receiverCount <- publish(channel, message).replicateZIO(numOfPublished).map(_.head)
            _             <- promise.succeed(())
            res           <- stream.join
          } yield assertTrue(
            res == Chunk(
              PushProtocol.Subscribe(channel, 1L),
              PushProtocol.Unsubscribe(channel, 0L)
            )
          ) && assertTrue(receiverCount == 0L)
        },
        test("unsubscribe response") {
          val channel = "foo"
          assertZIO(unsubscribe(channel).runHead)(
            isSome(
              isSubtype[PushProtocol.Unsubscribe](equalTo(PushProtocol.Unsubscribe(channel, 0L)))
            )
          )
        },
        test("punsubscribe response") {
          val pattern = "f*"
          assertZIO(pUnsubscribe(pattern).runHead)(
            isSome(
              isSubtype[PushProtocol.PUnsubscribe](equalTo(PushProtocol.PUnsubscribe(pattern, 0L)))
            )
          )
        },
        test("unsubscribe with empty param") {
          val channel1 = "foo"
          val channel2 = "bar"
          for {
            _                   <- subscribe(channel1).returning[String].runHead
            _                   <- subscribe(channel2).returning[String].runHead
            promise             <- Promise.make[RedisError, Unit]
            stream              <- unsubscribe().interruptWhen(promise).runCollect.fork
            _                   <- promise.succeed(())
            unsubscribeMessages <- stream.join
            numSubResponses     <- pubSubNumSub(channel1, channel2)
          } yield assertTrue(
            unsubscribeMessages == Chunk(
              PushProtocol.Unsubscribe(channel1, 1L),
              PushProtocol.Unsubscribe(channel2, 0L)
            )
          ) && assertTrue(
            numSubResponses == Chunk(
              NumSubResponse(channel1, 0L),
              NumSubResponse(channel2, 0L)
            )
          )
        }
      )
    )
}
