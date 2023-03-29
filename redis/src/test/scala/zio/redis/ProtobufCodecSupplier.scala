package zio.redis

import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

object ProtobufCodecSupplier extends CodecSupplier {
  def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec
}
