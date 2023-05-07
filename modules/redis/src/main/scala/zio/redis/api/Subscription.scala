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

package zio.redis.api

import zio.redis.ResultBuilder.ResultStreamBuilder1
import zio.redis._
import zio.redis.internal._
import zio.redis.options.PubSub.PubSubCallback
import zio.schema.Schema
import zio.stream.Stream
import zio.{Chunk, IO, ZIO}

trait Subscription extends SubscribeEnvironment {
  import Subscription._

  final def subscribe(channel: String): ResultStreamBuilder1[Id] =
    subscribeWithCallback(channel)(emptyCallback, emptyCallback)

  final def subscribeWithCallback(channel: String)(
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ): ResultStreamBuilder1[Id] =
    new ResultStreamBuilder1[Id] {
      def returning[R: Schema]: Stream[RedisError, R] =
        RedisSubscriptionCommand(executor)
          .subscribe(
            Chunk.single(channel),
            onSubscribe,
            onUnsubscribe
          )
          .map(_._2)
    }

  final def subscribe(
    channel: String,
    channels: String*
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    subscribeWithCallback(channel, channels: _*)(emptyCallback, emptyCallback)

  final def subscribeWithCallback(channel: String, channels: String*)(
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    new ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] {
      def returning[R: Schema]: Stream[RedisError, (String, R)] =
        RedisSubscriptionCommand(executor).subscribe(
          Chunk.single(channel) ++ Chunk.fromIterable(channels),
          onSubscribe,
          onUnsubscribe
        )
    }

  final def pSubscribe(pattern: String): ResultStreamBuilder1[Id] =
    pSubscribeWithCallback(pattern)(emptyCallback, emptyCallback)

  final def pSubscribeWithCallback(
    pattern: String
  )(
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ): ResultStreamBuilder1[Id] =
    new ResultStreamBuilder1[Id] {
      def returning[R: Schema]: Stream[RedisError, R] =
        RedisSubscriptionCommand(executor)
          .pSubscribe(
            Chunk.single(pattern),
            onSubscribe,
            onUnsubscribe
          )
          .map(_._2)
    }

  final def pSubscribe(
    pattern: String,
    patterns: String*
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    pSubscribeWithCallback(pattern, patterns: _*)(emptyCallback, emptyCallback)

  final def pSubscribeWithCallback(
    pattern: String,
    patterns: String*
  )(
    onSubscribe: PubSubCallback,
    onUnsubscribe: PubSubCallback
  ): ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] =
    new ResultStreamBuilder1[({ type lambda[x] = (String, x) })#lambda] {
      def returning[R: Schema]: Stream[RedisError, (String, R)] =
        RedisSubscriptionCommand(executor).pSubscribe(
          Chunk.single(pattern) ++ Chunk.fromIterable(patterns),
          onSubscribe,
          onUnsubscribe
        )
    }

  final def unsubscribe(channels: String*): IO[RedisError, Unit] =
    RedisSubscriptionCommand(executor).unsubscribe(Chunk.fromIterable(channels))

  final def pUnsubscribe(patterns: String*): IO[RedisError, Unit] =
    RedisSubscriptionCommand(executor).pUnsubscribe(Chunk.fromIterable(patterns))

}

object Subscription {
  private lazy val emptyCallback = (_: String, _: Long) => ZIO.unit

  final val Subscribe    = "SUBSCRIBE"
  final val Unsubscribe  = "UNSUBSCRIBE"
  final val PSubscribe   = "PSUBSCRIBE"
  final val PUnsubscribe = "PUNSUBSCRIBE"
}
