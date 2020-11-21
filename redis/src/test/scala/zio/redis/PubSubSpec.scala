package zio.redis

import zio.test.Assertion._
import zio.test._

trait PubSubSpec extends BaseSpec {

  val pubSubSuite =
    suite("pubsub")(suite("publish subscribe")(testM("publish one message and receive it as subscriber") {
      val channel   = "channel_1"
      val msg       = "msg_1"
      val subStream = subscribe(channel)

      for {
        _      <- publish(channel, msg)
        msgGot <- subStream.runHead
      } yield assert(msgGot)(isSome(equalTo(msg)))
    }))
}
