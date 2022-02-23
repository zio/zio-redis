package zio.redis

import dev.profunktor.redis4cats.RedisCommands
import io.chrisdavenport.rediculous.RedisConnection
import laserdisc.fs2.RedisClient
import cats.effect.{IO => CIO}

package object benchmarks {
  type Redis4CatsClient[A] = RedisCommands[CIO, String, A]
  type LaserDiscClient     = RedisClient[CIO]
  type RediculousClient    = RedisConnection[CIO]
}
