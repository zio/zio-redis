package zio.redis.codec

import java.nio.charset.StandardCharsets

import zio.redis.RedisError.CodecError
import zio.schema.Schema
import zio.schema.StandardType.StringType
import zio.schema.codec.Codec
import zio.stream.ZTransducer
import zio.{ Chunk, ZIO }

object StringUtf8Codec extends Codec {
  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer.fromPush { (opt: Option[Chunk[A]]) =>
      ZIO.succeed(opt.map(values => values.flatMap(Encoder.encode(schema, _))).getOrElse(Chunk.empty))
    }

  override def encode[A](schema: Schema[A]): A => Chunk[Byte] = { a =>
    Encoder.encode(schema, a)
  }

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer.fromPush { (opt: Option[Chunk[Byte]]) =>
      ZIO.fromEither(opt.map(chunk => Decoder.decode(schema, chunk).map(Chunk(_))).getOrElse(Right(Chunk.empty)))
    }

  override def decode[A](schema: Schema[A]): Chunk[Byte] => Either[String, A] = { ch =>
    if (ch.isEmpty) Left("No bytes to decode")
    else Decoder.decode(schema, ch)
  }

  object Encoder {
    def encode[A](schema: Schema[A], value: A): Chunk[Byte] =
      schema match {
        case Schema.Primitive(StringType) => Chunk.fromArray(value.toString.getBytes(StandardCharsets.UTF_8))
        case _                            => throw CodecError("the codec support only strings")
      }
  }

  object Decoder {
    def decode[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, A] =
      schema match {
        case Schema.Primitive(StringType) => Right(new String(chunk.toArray, StandardCharsets.UTF_8).asInstanceOf[A])
        case _                            => Left("the codec support only strings")
      }
  }
}
