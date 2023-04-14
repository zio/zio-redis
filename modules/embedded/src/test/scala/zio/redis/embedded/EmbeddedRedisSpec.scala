/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.embedded

import zio._
import zio.redis._
import zio.schema.codec.{BinaryCodec, ProtobufCodec}
import zio.schema.{DeriveSchema, Schema}
import zio.test.Assertion._
import zio.test._

import java.util.UUID

object EmbeddedRedisSpec extends ZIOSpecDefault {

  final case class Item(id: UUID, name: String, quantity: Int)
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
    EmbeddedRedis.layer,
    ZLayer.succeed[CodecSupplier](new CodecSupplier {
      def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
    }),
    Redis.singleNode
  ) @@ TestAspect.silentLogging

}
