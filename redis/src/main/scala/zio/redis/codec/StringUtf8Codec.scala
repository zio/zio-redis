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

package zio.redis.codec

import zio.redis.RedisError.CodecError
import zio.schema.Schema
import zio.schema.StandardType.{DoubleType, IntType, LongType}
import zio.schema.codec.Codec
import zio.stream.ZPipeline
import zio.{Chunk, ZIO}

import java.nio.charset.StandardCharsets

private[redis] object StringUtf8Codec extends Codec {
  def encoder[A](schema: Schema[A]): ZPipeline[Any, Nothing, A, Byte] =
    ZPipeline.mapChunks(values => values.flatMap(Encoder.encode(schema, _)))

  def encode[A](schema: Schema[A]): A => Chunk[Byte] = { a =>
    Encoder.encode(schema, a)
  }

  def decoder[A](schema: Schema[A]): ZPipeline[Any, String, Byte, A] =
    ZPipeline.mapChunksZIO(chunk => ZIO.fromEither(Decoder.decode(schema, chunk).map(Chunk(_))))

  def decode[A](schema: Schema[A]): Chunk[Byte] => Either[String, A] = { ch =>
    Decoder.decode(schema, ch)
  }

  object Encoder {
    def encode[A](schema: Schema[A], value: A): Chunk[Byte] =
      schema match {
        case Schema.Primitive(_, _) => Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
        case _                      => throw CodecError("the codec support only primitives")
      }
  }

  object Decoder {
    def decode[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, A] = {
      def utf8String = new String(chunk.toArray, StandardCharsets.UTF_8)

      schema match {
        case Schema.Primitive(IntType, _)    => Right(utf8String.toInt.asInstanceOf[A])
        case Schema.Primitive(LongType, _)   => Right(utf8String.toLong.asInstanceOf[A])
        case Schema.Primitive(DoubleType, _) => Right(utf8String.toDouble.asInstanceOf[A])
        case Schema.Primitive(_, _)          => Right(utf8String.asInstanceOf[A])
        case _                               => Left("the codec support only primitives")
      }
    }
  }
}
