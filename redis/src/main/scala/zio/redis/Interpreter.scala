package zio.redis

import java.io.IOException
import java.net.{ InetSocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuilder

import zio._
import zio.clock.{ currentTime, sleep, Clock }
import zio.duration.{ durationInt, Duration }
import zio.redis.ConnectionType._
import zio.stm.{ STM, TQueue, TRef }

trait Interpreter {
  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {

    val ResponseBufferSize = 1024

    trait Service {
      def execute(command: Chunk[String], connectionType: ConnectionType): Task[String]
    }

    // TODO: parametrize time interval in which connections should be clean
    def live(config: PoolConfig): URLayer[Clock, RedisExecutor] =
      ZLayer.fromManaged {
        for {
          clock <- Managed.environment[Clock]
          cps   <- ConnectionPools(config)
          _     <- (cps.cleanIdle *> sleep(10.seconds)).forever.forkManaged
        } yield new Service {
          def execute(command: Chunk[String], connectionType: ConnectionType): Task[String] =
            cps.get(connectionType).provide(clock).use(_.send(command))
        }
      }

    private[this] final class Connection(
      channel: SocketChannel,
      response: ByteBuffer,
      val idleTime: TRef[Long]
    ) {
      def send(command: Chunk[String]): IO[RedisError, String] = {
        val exchange =
          IO.effect {
            try {
              unsafeSend(command)
              unsafeReceive()
            } catch {
              case t: Throwable => throw RedisError.ProtocolError(t.getMessage)
            }
          }

        exchange.refineToOrDie[RedisError]
      }

      val close: UIO[Unit] =
        IO.effect(channel.close()).orDie

      private def unsafeReceive(): String = {
        val builder      = ArrayBuilder.make[Array[Byte]]
        var readBytes    = 0
        var responseSize = 0

        // TODO: handle -1
        while (readBytes == 0 || readBytes == ResponseBufferSize) {
          channel.read(response)
          response.flip()

          readBytes = response.remaining()
          responseSize += readBytes

          val chunk = Array.ofDim[Byte](readBytes)

          response.get(chunk)

          builder += chunk

          response.clear()
        }

        val chunks = builder.result()
        val data   = Array.ofDim[Byte](responseSize)
        var i      = 0
        var j      = 0

        while (i < chunks.length) {
          val chunk = chunks(i)

          System.arraycopy(chunk, 0, data, j, chunk.length)

          i += 1
          j += chunk.length
        }

        new String(data, UTF_8)
      }

      private def unsafeSend(command: Chunk[String]): Unit = {
        val data     = command.mkString
        val envelope = s"*${command.length}\r\n$data"
        val buffer   = ByteBuffer.wrap(envelope.getBytes(UTF_8))

        while (buffer.hasRemaining())
          channel.write(buffer)
      }
    }

    private[this] final class ConnectionPools private (
      basePool: ConnectionPool,
      streamsPool: ConnectionPool,
      transactionsPool: ConnectionPool
    ) {
      def get(connectionType: ConnectionType): RManaged[Clock, Connection] =
        connectionType match {
          case Base         => basePool.get
          case Streams      => streamsPool.get
          case Transactions => transactionsPool.get
        }

      val cleanIdle: URIO[Clock, Unit] =
        for {
          _ <- basePool.cleanIdle
          _ <- streamsPool.cleanIdle
          _ <- transactionsPool.cleanIdle
        } yield ()
    }

    private[this] object ConnectionPools {
      def apply(cfg: PoolConfig): UManaged[ConnectionPools] =
        for {
          basePool         <- ConnectionPool(cfg.host, cfg.port, cfg.baseMaxSize, cfg.baseMaxIdleTimeout)
          streamsPool      <- ConnectionPool(cfg.host, cfg.port, cfg.streamsMaxSize, cfg.streamsMaxIdleTimeout)
          transactionsPool <-
            ConnectionPool(cfg.host, cfg.port, cfg.transactionsMaxSize, cfg.transactionsMaxIdleTimeout)
        } yield new ConnectionPools(basePool, streamsPool, transactionsPool)
    }

    // Add timeout and then create ConnectionPools
    private[this] final class ConnectionPool(
      host: String,
      port: Int,
      conns: TQueue[Connection],
      closed: TRef[Boolean],
      maxSize: Int,
      size: TRef[Int],
      maxIdleTimeout: Long
    ) {
      def get: ZManaged[Clock, IOException, Connection] = {
        val makeChannel =
          IO {
            val channel = SocketChannel.open(new InetSocketAddress(host, port))
            channel.configureBlocking(false)
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
            channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
            channel
          }

        val makeConnection =
          for {
            channel  <- makeChannel.refineToOrDie[IOException]
            idleTime <- TRef.makeCommit(0L)
            response  = ByteBuffer.allocate(ResponseBufferSize)
          } yield new Connection(channel, response, idleTime)

        val getConnection =
          for {
            createConn <- STM.atomically {
                            for {
                              connsEmpty <- conns.isEmpty
                              numOfConns <- size.get
                              create      = connsEmpty && numOfConns < maxSize
                              _          <- size.update(_ + 1).when(create)
                            } yield create
                          }
            conn       <- if (createConn) makeConnection.tapError(_ => size.update(_ - 1).commit)
                    else conns.take.commit
          } yield conn

        getConnection.toManaged { c =>
          for {
            now     <- currentTime(TimeUnit.MILLISECONDS)
            closing <- STM.atomically {
                         for {
                           closing <- closed.get
                           _       <- c.idleTime.set(now).when(!closing)
                           _       <- conns.offer(c).when(!closing)
                         } yield closing
                       }
            _       <- c.close.when(closing)
          } yield ()
        }
      }

      val close: UIO[Unit] =
        for {
          _         <- closed.set(true).commit
          idleConns <- conns.takeAll.commit
          _         <- ZIO.foreachPar_(idleConns)(_.close)
        } yield ()

      val cleanIdle: URIO[Clock, Unit] =
        for {
          now       <- currentTime(TimeUnit.MILLISECONDS)
          removeIdle = for {
                         optionConn <- conns.peekOption
                         conn       <- STM.fromOption(optionConn)
                         idleTime   <- conn.idleTime.get
                         timedout    = now - idleTime > maxIdleTimeout
                         _          <- conns.take.when(timedout)
                       } yield timedout
          _         <- STM.iterate(true)(ZIO.identityFn)(_ => removeIdle).commit.ignore
        } yield ()
    }

    private[this] object ConnectionPool {
      def apply(host: String, port: Int, maxSize: Int, maxIdleTimeout: Duration): UManaged[ConnectionPool] = {
        val makeCP = for {
          queue  <- TQueue.bounded[Connection](maxSize).commit
          size   <- TRef.makeCommit(0)
          closed <- TRef.makeCommit(false)
        } yield new ConnectionPool(host, port, queue, closed, maxSize, size, maxIdleTimeout.toMillis)

        Managed.make(makeCP)(_.close)
      }
    }
  }

}
