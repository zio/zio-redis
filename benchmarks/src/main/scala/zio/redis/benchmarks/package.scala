package zio.redis

import cats.effect.{IO => CIO}
import dev.profunktor.redis4cats.RedisCommands
import io.chrisdavenport.rediculous
import laserdisc.fs2.RedisClient

package object benchmarks {
  type Redis4CatsClient[A] = RedisCommands[CIO, String, A]
  type LaserDiscClient     = RedisClient[CIO]
  type RediculousClient    = rediculous.RedisConnection[CIO]
}
