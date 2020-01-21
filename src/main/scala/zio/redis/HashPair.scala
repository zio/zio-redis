package zio.redis

import zio.Chunk

final case class HashPair(field: String, value: Chunk[Byte])
