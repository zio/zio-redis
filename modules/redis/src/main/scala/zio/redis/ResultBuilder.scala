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

import zio.IO
import zio.redis.ResultBuilder.NeedsReturnType
import zio.schema.Schema
import zio.stream.Stream

/**
 * The result of a command whose reply type the caller chooses. Call `returning` with the type to get the command's
 * result. `map` and `flatMap` do not compile before `returning` is called.
 */
sealed trait ResultBuilder {

  /**
   * Does not compile without `returning`. The compiler reports the message of the `implicitNotFound` annotation, which
   * is ``Use `returning[A]` to specify method's return type``. The library provides no `NeedsReturnType` instance,
   * so the body `???` is never reached.
   */
  final def map(f: Nothing => Any)(implicit nrt: NeedsReturnType): IO[Nothing, Nothing] = ???

  /**
   * Does not compile without `returning`, for the same reason as `map`.
   */
  final def flatMap(f: Nothing => Any)(implicit nrt: NeedsReturnType): IO[Nothing, Nothing] = ???
}

/**
 * The builders that a command returns while the caller has not yet chosen the reply type. Each builder's `returning`
 * method chooses it.
 */
object ResultBuilder {

  /**
   * Evidence that a `ResultBuilder` method was used without `returning`. No instance is provided, so such a call does
   * not compile.
   */
  @annotation.implicitNotFound("Use `returning[A]` to specify method's return type")
  final abstract class NeedsReturnType

  /**
   * A result with one reply type `R`, wrapped in `F` and in the effect type `G`. For example, `get` returns
   * `Option[R]`.
   */
  trait ResultBuilder1[+F[_], G[+_]] extends ResultBuilder {

    /**
     * Chooses `R` as the reply type. The reply is decoded with the codec from the `CodecSupplier` for `R`.
     */
    def returning[R: Schema]: G[F[R]]
  }

  /**
   * A result with two reply types, such as the result of `XREADGROUP`.
   */
  trait ResultBuilder2[+F[_, _], G[+_]] extends ResultBuilder {

    /**
     * Chooses `R1` and `R2` as the reply types. Each one is decoded with its own codec.
     */
    def returning[R1: Schema, R2: Schema]: G[F[R1, R2]]
  }

  /**
   * A result with three reply types, such as the result of `XAUTOCLAIM`.
   */
  trait ResultBuilder3[+F[_, _, _], G[+_]] extends ResultBuilder {

    /**
     * Chooses the three reply types. Each one is decoded with its own codec.
     */
    def returning[R1: Schema, R2: Schema, R3: Schema]: G[F[R1, R2, R3]]
  }

  /**
   * A result whose reply type is decoded with an `Output`, not with a `Schema`. The scripting commands `EVAL` and
   * `EVALSHA` use it, because their replies can have any shape.
   */
  trait ResultOutputBuilder[G[+_]] extends ResultBuilder {

    /**
     * Chooses `R` as the reply type, decoded with the `Output` in scope.
     */
    def returning[R: Output]: G[R]
  }

  /**
   * A stream of values of one type `R`, wrapped in `F`. The subscribe commands return it, with one element for each
   * received message.
   */
  trait ResultStreamBuilder1[+F[_]] extends ResultBuilder {

    /**
     * Chooses `R` as the type of the values. The stream fails with a `RedisError`.
     */
    def returning[R: Schema]: Stream[RedisError, F[R]]
  }
}
