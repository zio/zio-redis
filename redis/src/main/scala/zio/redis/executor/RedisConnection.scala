/*
 * Copyright 2021 John A. De Goes and the ZIO contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.redis.executor

import zio._
import zio.logging._
import zio.redis.{RedisError, RedisUri}
import zio.stream.Stream

import java.io.{EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

private[redis] trait RedisConnection {
  def read: Stream[IOException, Byte]
  def write(chunk: Chunk[Byte]): IO[IOException, Unit]
}

private[redis] final class RedisConnectionLive(
  readBuffer: ByteBuffer,
  writeBuffer: ByteBuffer,
  channel: AsynchronousSocketChannel
) extends RedisConnection {
  import RedisConnectionLive._

  val read: Stream[IOException, Byte] =
    Stream.repeatEffectChunkOption {
      val receive =
        for {
          _ <- IO.effectTotal(readBuffer.clear())
          _ <- closeWith[Integer](channel)(channel.read(readBuffer, null, _)).filterOrFail(_ >= 0)(new EOFException())
          chunk <- IO.effectTotal {
                     readBuffer.flip()
                     val count = readBuffer.remaining()
                     val array = Array.ofDim[Byte](count)
                     readBuffer.get(array)
                     Chunk.fromArray(array)
                   }
        } yield chunk

      receive.mapError {
        case _: EOFException => None
        case e: IOException  => Some(e)
      }
    }

  def write(chunk: Chunk[Byte]): IO[IOException, Unit] =
    IO.when(chunk.nonEmpty) {
      IO.effectSuspendTotal {
        writeBuffer.clear()
        val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
        writeBuffer.put(c.toArray)
        writeBuffer.flip()

        closeWith[Integer](channel)(channel.write(writeBuffer, null, _))
          .repeatWhile(_ => writeBuffer.hasRemaining)
          .zipRight(write(remainder))
      }
    }
}

private[redis] object RedisConnectionLive {

  lazy val layer: ZLayer[Logging with Has[RedisUri], RedisError.IOError, Has[RedisConnection]] =
    ZLayer.fromServiceManaged[RedisUri, Logging, RedisError.IOError, RedisConnection] { uri =>
      connect(new InetSocketAddress(uri.host, uri.port))
    }

  lazy val default: ZLayer[Logging, RedisError.IOError, Has[RedisConnection]] =
    ZLayer.succeed(RedisUri.Default) ++ ZLayer.identity[Logging] >>> layer

  private[redis] def connect(address: => SocketAddress): ZManaged[Logging, RedisError.IOError, RedisConnection] =
    (for {
      address     <- UIO(address).toManaged_
      makeBuffer   = IO.effectTotal(ByteBuffer.allocateDirect(ResponseBufferSize))
      readBuffer  <- makeBuffer.toManaged_
      writeBuffer <- makeBuffer.toManaged_
      channel     <- openChannel(address)
    } yield new RedisConnectionLive(readBuffer, writeBuffer, channel)).mapError(RedisError.IOError)

  private final val ResponseBufferSize = 1024

  private def completionHandler[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(IO.succeedNow(result))

      def failed(t: Throwable, u: Any): Unit =
        t match {
          case e: IOException => k(IO.fail(e))
          case _              => k(IO.die(t))
        }
    }

  private def closeWith[A](channel: Channel)(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    IO.effectAsyncInterrupt { k =>
      op(completionHandler(k))
      Left(IO.effect(channel.close()).ignore)
    }

  private def openChannel(address: SocketAddress): ZManaged[Logging, IOException, AsynchronousSocketChannel] =
    Managed.fromAutoCloseable {
      for {
        logger <- ZIO.service[Logger[String]]
        channel <- IO.effect {
                     val channel = AsynchronousSocketChannel.open()
                     channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                     channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                     channel
                   }
        _ <- closeWith[Void](channel)(channel.connect(address, null, _))
        _ <- logger.info(s"Connected to the redis server with address $address.")
      } yield channel
    }.refineToOrDie[IOException]
}
