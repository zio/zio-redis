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

package zio.redis

import zio.Chunk
import zio.redis.RespValue.BulkString
import zio.redis.codecs.CRC16
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

import java.nio.charset.StandardCharsets

sealed trait RespArgument {
  def value: RespValue.BulkString
}

object RespArgument {

  final case class Unknown(bytes: Chunk[Byte]) extends RespArgument {
    lazy val value: BulkString = RespValue.BulkString(bytes)
  }

  object Unknown {
    def apply(str: String): Unknown                                                = Unknown(Chunk.fromArray(str.getBytes(StandardCharsets.UTF_8)))
    def apply[A](data: A)(implicit codec: BinaryCodec, schema: Schema[A]): Unknown = Unknown(codec.encode(schema)(data))
  }

  final case class CommandName(str: String) extends RespArgument {
    lazy val value: BulkString = RespValue.bulkString(str)
  }

  final case class Literal(str: String) extends RespArgument {
    lazy val value: BulkString = RespValue.bulkString(str)
  }

  final case class Key(bytes: Chunk[Byte]) extends RespArgument {
    lazy val value: BulkString = RespValue.BulkString(bytes)

    lazy val asCRC16: Int = {
      val betweenBraces = bytes.dropWhile(b => b != '{').drop(1).takeWhile(b => b != '}')
      val key           = if (betweenBraces.isEmpty) bytes else betweenBraces
      CRC16.get(key)
    }
  }

  object Key {
    def apply[A](data: A)(implicit codec: BinaryCodec, schema: Schema[A]): Key = Key(codec.encode(schema)(data))
  }

  final case class Value(bytes: Chunk[Byte]) extends RespArgument {
    lazy val value: BulkString = RespValue.BulkString(bytes)
  }

  object Value {
    def apply[A](data: A)(implicit codec: BinaryCodec, schema: Schema[A]): Value = Value(codec.encode(schema)(data))
  }
}
