package zio.redis.codecs

import zio.redis.CodecSupplier
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

object ProtobufCodecSupplier extends CodecSupplier {
  implicit def codec[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
}
