package zio.redis.benchmarks

import cats.effect.{IO => CIO, Resource}
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s._
import dev.profunktor.redis4cats.RedisCommands
import dev.profunktor.redis4cats.data.RedisCodec
import fs2.io.net.Network
import io.chrisdavenport.rediculous.RedisConnection
import io.lettuce.core.ClientOptions
import laserdisc.fs2.RedisClient
import laserdisc.auto.autoRefine
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log.NoOp.instance
import dev.profunktor.redis4cats.codecs.Codecs
import dev.profunktor.redis4cats.codecs.splits._

trait QueryRunner[Client] {
  def unsafeRunWith(query: Client => CIO[Unit]): Unit
}

object QueryRunner {
  def apply[Client](implicit instance: QueryRunner[Client]): QueryRunner[Client] = instance

  implicit val laserDiscClientRunner: QueryRunner[LaserDiscClient] =
    new QueryRunner[LaserDiscClient] {
      def unsafeRunWith(f: LaserDiscClient => CIO[Unit]): Unit =
        Laserdisc.use(f).unsafeRunSync()
    }

  implicit val redicoulusRunner: QueryRunner[RediculousClient] =
    new QueryRunner[RediculousClient] {
      def unsafeRunWith(f: RediculousClient => CIO[Unit]): Unit =
        Redicoulus.use(f).unsafeRunSync()
    }

  implicit val redis4CatsStringRunner: QueryRunner[Redis4CatsClient[String]] =
    new QueryRunner[Redis4CatsClient[String]] {
      def unsafeRunWith(f: Redis4CatsClient[String] => CIO[Unit]): Unit =
        Redis4CatsString.use(f).unsafeRunSync()
    }

  implicit val redis4CatsLongRunner: QueryRunner[Redis4CatsClient[Long]] =
    new QueryRunner[Redis4CatsClient[Long]] {
      def unsafeRunWith(f: Redis4CatsClient[Long] => CIO[Unit]): Unit =
        Redis4CatsLong.use(f).unsafeRunSync()
    }

  private[this] final val RedisHost = "127.0.0.1"
  private[this] final val RedisPort = 6379

  private[this] final val Laserdisc: Resource[CIO, LaserDiscClient] = RedisClient[CIO].to(RedisHost, RedisPort)

  private[this] final val Redicoulus: Resource[CIO, RediculousClient] =
    RedisConnection.queued[CIO](Network[CIO], host"127.0.0.1", port"6379", maxQueued = 10000, workers = 2)

  private[this] final val Redis4CatsLong: Resource[CIO, RedisCommands[CIO, String, Long]] = {
    val longCodec = Codecs.derive(RedisCodec.Utf8, stringLongEpi)
    Redis[CIO].withOptions(s"redis://$RedisHost:$RedisPort", ClientOptions.create(), longCodec)
  }

  private[this] final val Redis4CatsString: Resource[CIO, RedisCommands[CIO, String, String]] =
    Redis[CIO].utf8(s"redis://$RedisHost:$RedisPort")
}
