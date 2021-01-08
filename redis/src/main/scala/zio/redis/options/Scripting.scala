package zio.redis.options

import zio.Chunk
import zio.redis.RedisEncoder

trait Scripting {

  sealed case class Script[K, A](lua: String, keys: Seq[K], args: Seq[A]) {
    def encodeKeys(implicit encoder: RedisEncoder[K]): Seq[Chunk[Byte]] = keys.map(k => encoder.encode(k))
    def encodeArgs(implicit encoder: RedisEncoder[A]): Seq[Chunk[Byte]] = args.map(a => encoder.encode(a))
  }
}
