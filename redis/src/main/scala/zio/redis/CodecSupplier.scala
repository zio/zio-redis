package zio.redis

import zio.redis.codecs.StringUtf8Codec
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

trait CodecSupplier {
  implicit def codec[A: Schema]: BinaryCodec[A]
}

private[redis] object StringCodecSupplier extends CodecSupplier {
  implicit def codec[A: Schema]: BinaryCodec[A] = StringUtf8Codec.codec
}
