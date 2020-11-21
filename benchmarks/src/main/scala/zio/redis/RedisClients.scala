package zio.redis

import cats.effect.{ Blocker, Resource, IO => CatsIO }
import dev.profunktor.redis4cats.RedisCommands
import fs2.io.tcp.SocketGroup
import io.chrisdavenport.rediculous.{ Redis, RedisConnection }
import laserdisc.fs2.RedisClient
import zio.redis.BenchmarkRuntime.{ RedisHost, RedisPort }

object RedisClients {
  import BenchmarkRuntime.cs
  import BenchmarkRuntime.timer

  type RedisIO[A] = Redis[CatsIO, A]

  type Redis4CatsClient = RedisCommands[CatsIO, String, String]
  type LaserDiskClient  = RedisClient[CatsIO]
  type RediculousClient = RedisConnection[CatsIO]

  trait QueryUnsafeRunner[F] {
    def unsafeRun(f: F => CatsIO[Unit]): Unit
  }

  implicit object LaserDiskClientRunner extends QueryUnsafeRunner[LaserDiskClient] {
    override def unsafeRun(f: LaserDiskClient => CatsIO[Unit]): Unit = laserDiskConnection.use(f).unsafeRunSync
  }

  implicit object RedicoulusClientRunner extends QueryUnsafeRunner[RediculousClient] {
    override def unsafeRun(f: RediculousClient => CatsIO[Unit]): Unit = redicoulusConnection.use(f).unsafeRunSync
  }

  implicit object Redis4CatsClientRunner extends QueryUnsafeRunner[Redis4CatsClient] {
    override def unsafeRun(f: Redis4CatsClient => CatsIO[Unit]): Unit = redis4CatsConnection.use(f).unsafeRunSync
  }

  private val redicoulusConnection: Resource[CatsIO, RediculousClient] =
    for {
      blocker <- Blocker[CatsIO]
      sg      <- SocketGroup[CatsIO](blocker)
      c       <- RedisConnection.queued[CatsIO](sg, RedisHost, RedisPort, maxQueued = 10000, workers = 2)
    } yield c

  import _root_.laserdisc.auto.autoRefine
  private val laserDiskConnection: Resource[CatsIO, LaserDiskClient] = RedisClient.to(RedisHost, RedisPort)

  import dev.profunktor.redis4cats.Redis
  import dev.profunktor.redis4cats.effect.Log.NoOp.instance
  private val redis4CatsConnection: Resource[CatsIO, RedisCommands[CatsIO, String, String]] =
    Redis[CatsIO].utf8(s"redis://$RedisHost:$RedisPort")
}
