package zio.redis

import java.io.{ EOFException, IOException }
import java.net.{ InetAddress, InetSocketAddress, SocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, AsynchronousSocketChannel, Channel, CompletionHandler }

import zio._
import zio.logging._
import zio.stream.Stream

trait Interpreter {

  import Interpreter._

  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {

    private[redis] val DefaultPort = 6379

    trait Service {

      def execute(command: Chunk[RespValue.BulkString], key: Option[String]): IO[RedisError, RespValue]

    }

    private val RequestQueueSize = 16

    private final class Live(
      reqQueue: Queue[Request],
      resQueue: Queue[Promise[RedisError, RespValue]],
      byteStream: Managed[IOException, ByteStream.ReadWriteBytes],
      logger: Logger[String]
    ) extends Service {

      override def execute(command: Chunk[RespValue.BulkString], key: Option[String]): IO[RedisError, RespValue] =
        Promise
          .make[RedisError, RespValue]
          .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

      private def sendWith(out: Chunk[Byte] => IO[IOException, Unit]): IO[RedisError, Unit] =
        reqQueue.takeBetween(1, Int.MaxValue).flatMap { reqs =>
          val bytes = Chunk.fromIterable(reqs).flatMap(req => RespValue.Array(req.command).serialize)
          out(bytes)
            .mapError(RedisError.IOError)
            .tapBoth(
              e => IO.foreach_(reqs)(_.promise.fail(e)),
              _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
            )
        }

      private def runReceive(inStream: Stream[IOException, Byte]): IO[RedisError, Unit] =
        inStream
          .mapError(RedisError.IOError)
          .transduce(RespValue.deserialize.toTransducer)
          .foreach(response => resQueue.take.flatMap(_.succeed(response)))

      /**
       * Opens a connection to the server and launches send and receive operations.
       * All failures are retried by opening a new connection.
       * Only exits by interruption or defect.
       */
      def run: IO[RedisError, Unit] =
        byteStream
          .mapError(RedisError.IOError)
          .use { rwBytes =>
            sendWith(rwBytes.write).forever race runReceive(rwBytes.read)
          }
          .tapError { e =>
            logger.warn(s"Reconnecting due to error: $e") *>
              resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))
          }
          .retryWhile(Function.const(true))
          .tapError(e => logger.error(s"Executor exiting: $e"))
    }

    private def fromBytestream: ZLayer[ByteStream with Logging, RedisError.IOError, RedisExecutor] =
      ZLayer.fromServicesManaged[ByteStream.Service, Logger[String], Any, RedisError.IOError, RedisExecutor.Service] {
        (byteStream: ByteStream.Service, logging: Logger[String]) =>
          for {
            reqQueue <- Queue.bounded[Request](RequestQueueSize).toManaged_
            resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
            live      = new Live(reqQueue, resQueue, byteStream.connect, logging)
            _        <- live.run.forkManaged
          } yield live
      }

    def live(address: SocketAddress): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.socket(address).mapError(RedisError.IOError)) >>> fromBytestream

    def live(host: String, port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.socket(host, port).mapError(RedisError.IOError)) >>> fromBytestream

    def loopback(port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.socketLoopback(port).mapError(RedisError.IOError)) >>> fromBytestream

    private[redis] val DefaultClusterPort: Int = 7000

    private final class LiveCluster(
      executors: NonEmptyChunk[(ClusterNode, RedisExecutor)],
      logger: Logger[String]
    ) extends Service {

      private val RedisClusterHashSlots: Int = 16384

      override def execute(
        command: Chunk[RespValue.BulkString],
        key: Option[String]
      ): zio.IO[RedisError, RespValue] = {
        val _        = (executors, logger, key, RedisClusterHashSlots)
        val executor = key.flatMap { k =>
          val slot = Crc16.crc16(k.getBytes()) % RedisClusterHashSlots
          executors.collectFirst {
            case (node, executor) if node.slots.find(_.covers(slot)).nonEmpty => executor
          }
        }.getOrElse(executors.head._2)
        executor.get
          .execute(command, None)
          .tapError(
            ZIO.whenCase(_) {
              // FIXME handle retry for Moved
              case RedisError.Moved(err) => logger.error(s"slot moved $err")
            }
          )
      }
    }
    // builds a ClusterExecutor from startup nodes
    // fetches topology and build the connected nodes
    // issue here is it ignores unconnected nodes
    // this need to be refreshed also during the lifetime of the ClusterExecutor
    def fromStartupNodes(first: SocketAddress, rest: SocketAddress*): ZLayer[Logging, RedisError, RedisExecutor] =
      ZLayer.fromFunctionManaged { logger =>
        def sequence[R, E, A](c: NonEmptyChunk[ZLayer[R, E, A]]) =
          c.tail
            .foldLeft(c.head.map(NonEmptyChunk.single))((r, l) => r.zipWithPar(l)(_ :+ _))

        val loggerLayer  = ZLayer.succeed(logger.get)
        val startupNodes = NonEmptyChunk(first, rest: _*).map {
          loggerLayer >>> zio.redis.RedisExecutor.live(_)
        }

        for {
          _                <- logger.get.info(s"trying to connect to: ${first}").toManaged_
          topologyFetchers <- sequence(startupNodes).build.map(_.map(e => clusterNodes().provide(e)))
          // FIXME these ones above will be kept until the service is stopped we need them temporarily
          topology         <- ZIO.firstSuccessOf(topologyFetchers.head, topologyFetchers.tail).toManaged_
          connected        <- topology
                         .filter(_.isConnected)
                         .nonEmptyOrElse[Managed[RedisError, NonEmptyChunk[ClusterNode]]](
                           ZManaged.fail(RedisError.ProtocolError("unable to reach cluster from startup nodes"))
                         )(ZManaged.succeed(_))
          layers            = connected
                     .map(cn => (loggerLayer >>> RedisExecutor.live(cn.ip, cn.port)).map(cn -> _))
          _                <- logger.get.info(s"cluster topology $connected").toManaged_
          nodes            <- sequence(layers).build

        } yield new LiveCluster(nodes, logger.get)
      }

    def localCluster(port: Int = DefaultClusterPort): ZLayer[Logging, RedisError, RedisExecutor] =
      ZLayer.fromFunctionManaged(logging =>
        for {
          socket <- IO.effectTotal(new InetSocketAddress(InetAddress.getLoopbackAddress, port)).toManaged_
          res    <- fromStartupNodes(socket).build.provide(logging)
        } yield res.get
      )

  }

  private type ByteStream = Has[ByteStream.Service]

  private[redis] object ByteStream {

    val ResponseBufferSize = 1024

    def connect: ZManaged[ByteStream, IOException, ReadWriteBytes] = ZManaged.accessManaged(_.get.connect)

    trait Service {

      val connect: Managed[IOException, ReadWriteBytes]

    }

    trait ReadWriteBytes {

      def read: Stream[IOException, Byte]

      def write(chunk: Chunk[Byte]): IO[IOException, Unit]

    }

    private def completionHandlerCallback[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
      new CompletionHandler[A, Any] {
        def completed(result: A, u: Any): Unit = k(IO.succeedNow(result))

        def failed(t: Throwable, u: Any): Unit =
          t match {
            case e: IOException => k(IO.fail(e))
            case _              => k(IO.die(t))
          }
      }

    private def effectAsyncChannel[C <: Channel, A](
      channel: C
    )(op: C => CompletionHandler[A, Any] => Any): IO[IOException, A] =
      IO.effectAsyncInterrupt { k =>
        op(channel)(completionHandlerCallback(k))
        Left(IO.effect(channel.close()).ignore)
      }

    def socket(host: String, port: Int): ZLayer[Logging, IOException, ByteStream] =
      socket(IO.effectTotal(new InetSocketAddress(host, port)))

    def socket(address: SocketAddress): ZLayer[Logging, IOException, ByteStream] = socket(UIO.succeed(address))

    def socketLoopback(port: Int): ZLayer[Logging, IOException, ByteStream] =
      socket(IO.effectTotal(new InetSocketAddress(InetAddress.getLoopbackAddress, port)))

    private def socket(getAddress: UIO[SocketAddress]): ZLayer[Logging, IOException, ByteStream] = {
      val makeBuffer = IO.effectTotal(ByteBuffer.allocateDirect(ResponseBufferSize))

      ZLayer.fromServiceM { logger =>
        for {
          address     <- getAddress
          readBuffer  <- makeBuffer
          writeBuffer <- makeBuffer
        } yield new Connection(address, readBuffer, writeBuffer, logger)
      }
    }

    private final class Connection(
      address: SocketAddress,
      readBuffer: ByteBuffer,
      writeBuffer: ByteBuffer,
      logger: Logger[String]
    ) extends Service {

      private def openChannel: Managed[IOException, AsynchronousSocketChannel] = {
        val make = for {
          channel <- IO.effect {
                       val channel = AsynchronousSocketChannel.open()
                       channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                       channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                       channel
                     }
          _       <- effectAsyncChannel[AsynchronousSocketChannel, Void](channel)(c => c.connect(address, null, _))
          _       <- logger.info("Connected to the redis server.")
        } yield channel
        Managed.fromAutoCloseable(make).refineToOrDie[IOException]
      }

      override val connect: Managed[IOException, ReadWriteBytes] =
        openChannel.map { channel =>
          val readChunk =
            (for {
              _     <- IO.effectTotal(readBuffer.clear())
              _     <- effectAsyncChannel[AsynchronousByteChannel, Integer](channel)(c => c.read(readBuffer, null, _))
                     .filterOrFail(_ >= 0)(new EOFException())
              chunk <- IO.effectTotal {
                         readBuffer.flip()
                         val count = readBuffer.remaining()
                         val array = Array.ofDim[Byte](count)
                         readBuffer.get(array)
                         Chunk.fromArray(array)
                       }
            } yield chunk).mapError {
              case _: EOFException => None
              case e: IOException  => Some(e)
            }

          @inline
          def writeChunk(chunk: Chunk[Byte]): IO[IOException, Unit] =
            IO.when(chunk.nonEmpty) {
              IO.effectTotal {
                writeBuffer.clear()
                val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
                writeBuffer.put(c.toArray)
                writeBuffer.flip()
                remainder
              }.flatMap { remainder =>
                effectAsyncChannel[AsynchronousByteChannel, Integer](channel)(c => c.write(writeBuffer, null, _))
                  .repeatWhileM(_ => IO.effectTotal(writeBuffer.hasRemaining))
                  .zipRight(writeChunk(remainder))
              }
            }

          new ReadWriteBytes {
            def read: Stream[IOException, Byte] = Stream.repeatEffectChunkOption(readChunk)

            def write(chunk: Chunk[Byte]): IO[IOException, Unit] = writeChunk(chunk)
          }
        }

    }

  }

}

object Interpreter {

  private final case class Request(
    command: Chunk[RespValue.BulkString],
    promise: Promise[RedisError, RespValue]
  )

}
