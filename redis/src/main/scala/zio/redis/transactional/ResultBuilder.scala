package zio.redis.transactional

import zio.ZIO
import zio.redis.transactional.ResultBuilder.NeedsReturnType
import zio.redis.{Output, RedisError}
import zio.schema.Schema

sealed trait ResultBuilder {

  final def map(f: Nothing => Any)(implicit nrt: NeedsReturnType): RedisTransaction[Nothing] = ???

  final def flatMap(f: Nothing => Any)(implicit nrt: NeedsReturnType): RedisTransaction[Nothing] = ???
}

object ResultBuilder {

  @annotation.implicitNotFound("Use `returning[A]` to specify method's return type")
  final abstract class NeedsReturnType

  trait ResultBuilder1[+F[_]] extends ResultBuilder {
    def returning[R: Schema]: RedisTransaction[F[R]]
  }

  trait ResultBuilder2[+F[_, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema]: RedisTransaction[F[R1, R2]]
  }

  trait ResultBuilder3[+F[_, _, _]] extends ResultBuilder {
    def returning[R1: Schema, R2: Schema, R3: Schema]: RedisTransaction[F[R1, R2, R3]]
  }

  trait ResultOutputBuilder extends ResultBuilder {
    def returning[R: Output]: ZIO[Redis, RedisError, R]
  }
}
