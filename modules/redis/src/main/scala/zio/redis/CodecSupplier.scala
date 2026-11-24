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

import zio.redis.internal.Utf8Codec
import zio.schema.Schema
import zio.schema.codec.BinaryCodec

/**
 * Chooses the codec that converts values to bytes and back. Commands use the codec that `get` returns for the type of
 * each key, value and reply they read or write.
 *
 * {{{
 * import zio.schema.codec.BinaryCodec
 *
 * val codec: BinaryCodec[Int] = CodecSupplier.utf8.get[Int]
 * }}}
 *
 * @see [[https://zio.dev/zio-redis/codecs/ Codecs]]
 */
trait CodecSupplier {
  /**
   * Returns the codec for values of type `A`. The `Schema` of `A` decides how a value is converted.
   */
  def get[A: Schema]: BinaryCodec[A]
}

/**
 * Supplies the built-in codec suppliers.
 */
object CodecSupplier {
  /**
   * A supplier of the UTF-8 codec for primitive values. A value is written with its `toString`. `Int`, `Long` and
   * `Double` are read with `toInt`, `toLong` and `toDouble`, so a text that is not a number of that type fails the read
   * with `NumberFormatException`. That exception is thrown, not returned as a `CodecError`. Other primitive types, for
   * example `Boolean`, `Short` and `Float`, are read as a `String`, so using the value as its own type fails with
   * `ClassCastException`. A value of any other type, such as a list, fails with `CodecError` and the message `the codec
   * support only primitives`.
   */
  def utf8: CodecSupplier = new CodecSupplier {
    def get[A: Schema]: BinaryCodec[A] = Utf8Codec.codec
  }
}
