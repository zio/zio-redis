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

package zio.redis

import zio._
import zio.stream.{Stream, ZStream}

import java.io.{EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

private[redis] final class RedisConnection(
  readBuffer: ByteBuffer,
  writeBuffer: ByteBuffer,
  channel: AsynchronousSocketChannel
) {
  import RedisConnection._

  val read: Stream[IOException, Byte] =
    ZStream.repeatZIOChunkOption {
      val receive =
        for {
          _ <- ZIO.succeed(readBuffer.clear())
          _ <- closeWith[Integer](channel)(channel.read(readBuffer, null, _)).filterOrFail(_ >= 0)(new EOFException())
          chunk <- ZIO.succeed {
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

  def write(chunk: Chunk[Byte]): IO[IOException, Option[Unit]] =
    ZIO.when(chunk.nonEmpty) {
      ZIO.suspendSucceed {
        writeBuffer.clear()
        val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
        writeBuffer.put(c.toArray)
        writeBuffer.flip()

        closeWith[Integer](channel)(channel.write(writeBuffer, null, _))
          .repeatWhile(_ => writeBuffer.hasRemaining)
          .zipRight(write(remainder))
          .map(_.getOrElse(()))
      }
    }
}

private[redis] object RedisConnection {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisConnection] =
    ZLayer.scoped(ZIO.serviceWithZIO[RedisConfig](create))

  lazy val local: ZLayer[Any, RedisError.IOError, RedisConnection] =
    ZLayer.succeed(RedisConfig.Local) >>> layer

  def create(uri: RedisConfig): ZIO[Scope, RedisError.IOError, RedisConnection] =
    connect(new InetSocketAddress(uri.host, uri.port))

  def connect(address: => SocketAddress): ZIO[Scope, RedisError.IOError, RedisConnection] =
    (for {
      address     <- ZIO.succeed(address)
      makeBuffer   = ZIO.succeed(ByteBuffer.allocateDirect(ResponseBufferSize))
      readBuffer  <- makeBuffer
      writeBuffer <- makeBuffer
      channel     <- openChannel(address)
      _           <- logScopeFinalizer("Redis connection is closed")
    } yield new RedisConnection(readBuffer, writeBuffer, channel)).mapError(RedisError.IOError(_))

  private final val ResponseBufferSize = 1024

  private def completionHandler[A](k: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, u: Any): Unit = k(ZIO.succeed(result))

      def failed(t: Throwable, u: Any): Unit =
        t match {
          case e: IOException => k(ZIO.fail(e))
          case _              => k(ZIO.die(t))
        }
    }

  private def closeWith[A](channel: Channel)(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    ZIO.asyncInterrupt { k =>
      op(completionHandler(k))
      Left(ZIO.attempt(channel.close()).ignore)
    }

  private def openChannel(address: SocketAddress): ZIO[Scope, IOException, AsynchronousSocketChannel] =
    ZIO.fromAutoCloseable {
      for {
        channel <- ZIO.attempt {
                     val channel = AsynchronousSocketChannel.open()
                     channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
                     channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
                     channel
                   }
        _ <- closeWith[Void](channel)(channel.connect(address, null, _))
        _ <- ZIO.logInfo(s"Connected to the redis server with address $address.")
      } yield channel
    }.refineToOrDie[IOException]
}
