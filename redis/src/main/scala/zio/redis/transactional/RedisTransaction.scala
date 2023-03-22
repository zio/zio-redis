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

package zio.redis.transactional

import zio.redis.Output.QueuedOutput
import zio.redis.{Output, RedisCommand, RedisError}
import zio.{ZIO, Zippable}

sealed trait RedisTransaction[+Out] { self =>
  import RedisTransaction._

  def commit: ZIO[Redis, RedisError, Out] =
    ZIO.serviceWithZIO[Redis] { redis =>
      self match {
        case Single(command, in) =>
          command.run(in)
        case _ =>
          redis._multi
            .run(())
            .zipRight(
              run.flatMap(output => redis._exec(output).run(()))
            )
      }
    }

  def zip[A](that: RedisTransaction[A])(implicit zippable: Zippable[Out, A]): RedisTransaction[zippable.Out] =
    Zip(this, that).map { case (left, right) => zippable.zip(left, right) }

  def zipLeft[A](that: RedisTransaction[A]): RedisTransaction[Out] =
    ZipLeft(this, that)

  def zipRight[A](that: RedisTransaction[A]): RedisTransaction[A] =
    ZipRight(this, that)

  def map[A](f: Out => A): RedisTransaction[A] = Map(self, f)

  private[transactional] def run: ZIO[Redis, RedisError, Output[Out]]
}

object RedisTransaction {
  final def single[In, Out](command: RedisCommand[In, Out], in: In): RedisTransaction[Out] =
    Single(command, in)

  private[transactional] final case class Single[In, Out](
    command: RedisCommand[In, Out],
    in: In
  ) extends RedisTransaction[Out] {
    def run: ZIO[Redis, RedisError, Output[Out]] =
      command.executor
        .execute(command.resp(in))
        .flatMap(out => ZIO.attempt(QueuedOutput.unsafeDecode(out)(command.codec)))
        .refineToOrDie[RedisError]
        .as(command.output)
  }

  private[transactional] final case class Zip[A, B](
    left: RedisTransaction[A],
    right: RedisTransaction[B]
  ) extends RedisTransaction[(A, B)] {
    def run: ZIO[Redis, RedisError, Output[(A, B)]] =
      left.run.zip(right.run).map(outputs => Output.Zip(outputs._1, outputs._2))
  }

  private[transactional] final case class ZipLeft[A, B](
    left: RedisTransaction[A],
    right: RedisTransaction[B]
  ) extends RedisTransaction[A] {
    def run: ZIO[Redis, RedisError, Output[A]] =
      left.run.zip(right.run).map(outputs => Output.ZipLeft(outputs._1, outputs._2))
  }

  private[transactional] final case class ZipRight[A, B](
    left: RedisTransaction[A],
    right: RedisTransaction[B]
  ) extends RedisTransaction[B] {
    def run: ZIO[Redis, RedisError, Output[B]] =
      left.run.zip(right.run).map(outputs => Output.ZipRight(outputs._1, outputs._2))
  }

  private[transactional] final case class Map[A, B](
    transaction: RedisTransaction[A],
    f: A => B
  ) extends RedisTransaction[B] {
    def run: ZIO[Redis, RedisError, Output[B]] =
      transaction.run.map(output => output.map(out => f(out)))
  }
}
