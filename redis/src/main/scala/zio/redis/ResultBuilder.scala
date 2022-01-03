package zio.redis

import zio.ZIO
import zio.redis.ResultBuilder.NeedsReturnType
import zio.schema.Schema

sealed trait ResultBuilder {

  final def map(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???

  final def flatMap(f: Nothing => Any)(implicit nrt: NeedsReturnType): ZIO[Any, Nothing, Nothing] = ???
}

object ResultBuilder {

  @annotation.implicitNotFound("Use `returning[A]` to specify method's return type")
  final abstract class NeedsReturnType

  trait ResultBuilder1[+F[_]] extends ResultBuilder {
    def returning[R: Schema]: ZIO[RedisExecutor, RedisError, F[R]]
  }

  trait ResultBuilder2[+F[_, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema]: ZIO[RedisExecutor, RedisError, F[R1, R2]]
  }

  trait ResultBuilder3[+F[_, _, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema, R3: Schema]: ZIO[RedisExecutor, RedisError, F[R1, R2, R3]]
  }
}
