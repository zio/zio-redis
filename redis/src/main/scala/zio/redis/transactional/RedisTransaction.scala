package zio.redis.transactional

import zio.ZIO
import zio.redis.Input.NoInput
import zio.redis.Output.{QueuedOutput, UnitOutput}
import zio.redis.{Output, RedisCommand, RedisError}

sealed trait RedisTransaction[+Out] {
  import RedisTransaction._

  def commit: ZIO[Redis, RedisError, Out] =
    ZIO.serviceWithZIO[Redis] { redis =>
      this match {
        case Single(command, in) =>
          RedisCommand(command.name, command.input, command.output, redis.codec, redis.executor)
            .run(in)
        case _ =>
          RedisCommand(Commands.Multi, NoInput, UnitOutput, redis.codec, redis.executor)
            .run(())
            .zipRight(
              run.flatMap(output => RedisCommand(Commands.Exec, NoInput, output, redis.codec, redis.executor).run(()))
            )
      }
    }

  def zip[A](that: RedisTransaction[A]): RedisTransaction[(Out, A)] =
    Zip(this, that)

  def zipLeft[A](that: RedisTransaction[A]): RedisTransaction[Out] =
    ZipLeft(this, that)

  def zipRight[A](that: RedisTransaction[A]): RedisTransaction[A] =
    ZipRight(this, that)

  private[transactional] def run: ZIO[Redis, RedisError, Output[Out]]
}

object RedisTransaction {
  def single[In, Out](command: RedisCommand[In, Out], in: In): RedisTransaction[Out] =
    Single(command, in)

  private[transactional] final case class Single[In, Out](
    command: RedisCommand[In, Out],
    in: In
  ) extends RedisTransaction[Out] {
    def run: ZIO[Redis, RedisError, Output[Out]] =
      RedisCommand[In, Unit](command.name, command.input, QueuedOutput, command.codec, command.executor)
        .run(in)
        .as(command.output)
  }

  private[transactional] final case class Zip[A, B](
    left: RedisTransaction[A],
    right: RedisTransaction[B]
  ) extends RedisTransaction[(A, B)] {
    def run: ZIO[Redis, RedisError, Output[(A, B)]] =
      left.run
        .zip(right.run)
        .map { outputs =>
          Output.Zip(outputs._1, outputs._2)
        }
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

  private object Commands {
    val Multi = "Multi"
    val Exec  = "Exec"
  }

}
