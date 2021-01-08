package zio.redis.api

import zio.ZIO
import zio.redis.Input.EvalInput
import zio.redis._
import Scripting._

trait Scripting {

  def eval[K: Input, A: Input, R: Output](e: Script[K, A]): ZIO[RedisExecutor, RedisError, R] =
    evalCommand[K, A, R].run(e)
}

private[redis] object Scripting {

  final def evalCommand[K: Input, A: Input, R: Output]: RedisCommand[Script[K, A], R] =
    RedisCommand("EVAL", EvalInput[K, A](implicitly[Input[K]], implicitly[Input[A]]), implicitly[Output[R]])
}
