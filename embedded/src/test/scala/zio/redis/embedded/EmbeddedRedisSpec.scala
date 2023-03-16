package zio.redis.embedded

import zio._
import zio.redis._
import zio.schema.{DeriveSchema, Schema}
import zio.schema.codec.{BinaryCodec, ProtobufCodec}
import zio.test._
import zio.test.Assertion._

import java.util.UUID

object EmbeddedRedisSpec extends ZIOSpecDefault {

  final case class Item private (id: UUID, name: String, quantity: Int)
  object Item {
    implicit val itemSchema: Schema[Item] = DeriveSchema.gen[Item]
  }

  def spec = suite("EmbeddedRedis should")(
    test("set and get values") {
      for {
        redis <- ZIO.service[Redis]
        item   = Item(UUID.randomUUID, "foo", 2)
        _     <- redis.set(s"item.${item.id.toString}", item)
        found <- redis.get(s"item.${item.id.toString}").returning[Item]
      } yield assert(found)(isSome(equalTo(item)))
    }
  ).provideShared(
    EmbeddedRedis.layer.orDie,
    RedisExecutor.layer.orDie,
    ZLayer.succeed[BinaryCodec](ProtobufCodec),
    Redis.layer
  ) @@ TestAspect.silentLogging

}
