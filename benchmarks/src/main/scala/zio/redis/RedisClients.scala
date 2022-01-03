package zio.redis

import cats.effect.unsafe.implicits.global
import cats.effect.{IO => CatsIO, _}
import com.comcast.ip4s._
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.data.RedisCodec
import fs2.io.net.Network
import io.chrisdavenport.rediculous.RedisConnection
import io.lettuce.core.ClientOptions
import laserdisc.fs2.RedisClient

trait RedisClients {
  import RedisClients._

  type Redis4CatsClient[V] = RedisCommands[CatsIO, String, V]
  type LaserDiscClient     = RedisClient[CatsIO]
  type RediculousClient    = RedisConnection[CatsIO]

  trait QueryUnsafeRunner[F] {
    def unsafeRun(f: F => CatsIO[Unit]): Unit
  }

  implicit object LaserDiscClientRunner extends QueryUnsafeRunner[LaserDiscClient] {
    override def unsafeRun(f: LaserDiscClient => CatsIO[Unit]): Unit = laserDiskConnection.use(f).unsafeRunSync()
  }

  implicit object RedicoulusClientRunner extends QueryUnsafeRunner[RediculousClient] {
    override def unsafeRun(f: RediculousClient => CatsIO[Unit]): Unit = redicoulusConnection.use(f).unsafeRunSync()
  }

  implicit object Redis4CatsClientRunnerString extends QueryUnsafeRunner[Redis4CatsClient[String]] {
    override def unsafeRun(f: Redis4CatsClient[String] => CatsIO[Unit]): Unit =
      redis4CatsConnectionString.use(f).unsafeRunSync()
  }

  implicit object Redis4CatsClientRunnerLong extends QueryUnsafeRunner[Redis4CatsClient[Long]] {
    override def unsafeRun(f: Redis4CatsClient[Long] => CatsIO[Unit]): Unit =
      redis4CatsConnectionLong.use(f).unsafeRunSync()
  }

  private val redicoulusConnection: Resource[CatsIO, RediculousClient] =
    RedisConnection.queued[CatsIO](Network[CatsIO], host"127.0.0.1", port"6379", maxQueued = 10000, workers = 2)

  import _root_.laserdisc.auto.autoRefine
  private val laserDiskConnection: Resource[CatsIO, LaserDiscClient] = RedisClient[CatsIO].to(RedisHost, RedisPort)

  import dev.profunktor.redis4cats.Redis
  import dev.profunktor.redis4cats.effect.Log.NoOp.instance
  private val redis4CatsConnectionString: Resource[CatsIO, RedisCommands[CatsIO, String, String]] =
    Redis[CatsIO].utf8(s"redis://$RedisHost:$RedisPort")

  private val redis4CatsConnectionLong: Resource[CatsIO, RedisCommands[CatsIO, String, Long]] = {
    import dev.profunktor.redis4cats.codecs.Codecs
    import dev.profunktor.redis4cats.codecs.splits._

    val longCodec: RedisCodec[String, Long] = Codecs.derive(RedisCodec.Utf8, stringLongEpi)
    Redis[CatsIO].withOptions(s"redis://$RedisHost:$RedisPort", ClientOptions.create(), longCodec)
  }
}

object RedisClients {
  private final val RedisHost = "127.0.0.1"
  private final val RedisPort = 6379
}
