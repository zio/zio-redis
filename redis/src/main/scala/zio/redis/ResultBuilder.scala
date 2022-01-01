package zio.redis

import zio.ZIO
import zio.schema.Schema

sealed trait ResultBuilder

trait ResultSchemaBuilder extends ResultBuilder {
  def returning[R: Schema]: ZIO[RedisExecutor, RedisError, R]
}

trait ResultSchemaBuilder1[F[_]] extends ResultBuilder {
  def returning[R: Schema]: ZIO[RedisExecutor, RedisError, F[R]]
}

trait ResultSchemaBuilder2[F[_, _]] extends ResultBuilder {
  def returning[R1: Schema, R2: Schema]: ZIO[RedisExecutor, RedisError, F[R1, R2]]
}

trait ResultSchemaBuilder3[F[_, _, _]] extends ResultBuilder {
  def returning[R1: Schema, R2: Schema, R3: Schema]: ZIO[RedisExecutor, RedisError, F[R1, R2, R3]]
}

trait ResultOutputBuilder extends ResultBuilder {
  def returning[R: Output]: ZIO[RedisExecutor, RedisError, R]
}
