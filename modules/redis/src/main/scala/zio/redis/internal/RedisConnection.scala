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

package zio.redis.internal

import tlschannel.ClientTlsChannel
import tlschannel.async.{AsynchronousTlsChannel, AsynchronousTlsChannelGroup}
import zio._
import zio.redis._
import zio.stream.{Stream, ZStream}

import java.io.{EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels._
import java.security.cert.X509Certificate
import java.util.Arrays
import javax.net.ssl.{SNIHostName, SSLContext, TrustManager, X509TrustManager}

private[redis] trait RedisConnection {
  def read: Stream[IOException, Byte]

  def reconnect: IO[IOException, Unit]

  def write(chunk: Chunk[Byte]): IO[IOException, Option[Unit]]
}

private[redis] object RedisConnection {
  lazy val layer: ZLayer[RedisConfig, RedisError.IOError, RedisConnection] =
    ZLayer.scoped(ZIO.serviceWithZIO[RedisConfig](create))

  lazy val local: ZLayer[Any, RedisError.IOError, RedisConfig & RedisConnection] =
    ZLayer.make[RedisConfig & RedisConnection](ZLayer.succeed(RedisConfig.Local), layer)

  private def create(config: RedisConfig): ZIO[Scope, RedisError.IOError, RedisConnection] = {
    val makeBuffer = ZIO.succeed(ByteBuffer.allocateDirect(ResponseBufferSize))
    for {
      readBuffer  <- makeBuffer
      writeBuffer <- makeBuffer
      scope       <- ZIO.scope
      channelRef  <-
        ScopedRef
          .fromAcquire(
            connect(new InetSocketAddress(config.host, config.port), config.sni, config.ssl, config.verifyCertificate)
              .mapError(RedisError.IOError(_))
          )
      _           <- logScopeFinalizer("Redis connection is closed")
    } yield new RedisConnection {
      def read: Stream[IOException, Byte] =
        ZStream.repeatZIOChunkOption {
          val receive =
            for {
              _       <- ZIO.succeed(readBuffer.clear())
              channel <- channelRef.get
              _       <-
                closeWith[Integer](channel)(channel.read(readBuffer, null, _)).filterOrFail(_ >= 0)(new EOFException())
              chunk   <- ZIO.succeed {
                           readBuffer.flip()
                           val count = readBuffer.remaining()
                           val array = Array.ofDim[Byte](count)
                           readBuffer.get(array)
                           Chunk.fromArray(array)
                         }
            } yield chunk

          receive.mapError {
            case e: EOFException => Some(new IOException(e))
            case e: IOException  => Some(e)
          }
        }

      def reconnect: IO[IOException, Unit] =
        channelRef
          .set(
            connect(new InetSocketAddress(config.host, config.port), config.sni, config.ssl, config.verifyCertificate)
          )
          .provideLayer(ZLayer.succeed(scope))

      def write(chunk: Chunk[Byte]): IO[IOException, Option[Unit]] =
        ZIO.when(chunk.nonEmpty) {
          channelRef.get.flatMap { channel =>
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
    }
  }

  def connect(
    address: => SocketAddress,
    sni: Option[String],
    ssl: Boolean,
    verifyCertificate: Boolean = true
  ): ZIO[Scope, IOException, AsynchronousByteChannel] =
    if (ssl) openTlsChannel(address, sni, verifyCertificate) else openChannel(address)

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
        _       <- closeWith[Void](channel)(channel.connect(address, null, _))
        _       <- ZIO.logInfo(s"Connected to the redis server with address $address.")
      } yield channel
    }.refineToOrDie[IOException]

  private def openTlsChannel(
    address: SocketAddress,
    sni: Option[String],
    verifyCertificate: Boolean
  ): ZIO[Scope, IOException, AsynchronousTlsChannel] =
    ZIO.fromAutoCloseable {
      for {
        channel <- createAsynchronousTlsChannel(address, sni, verifyCertificate)
        _       <- closeWith[Integer](channel)(channel.read(ByteBuffer.allocate(0), null, _))
        _       <- ZIO.logInfo(s"Connected to the redis server with address $address.")
      } yield channel
    }.refineToOrDie[IOException]

  private def createAsynchronousTlsChannel(
    address: SocketAddress,
    sni: Option[String],
    verifyCertificate: Boolean
  ): Task[AsynchronousTlsChannel] =
    ZIO.attempt {
      val sslContext        =
        if (verifyCertificate) {
          SSLContext.getDefault()
        } else {
          val ctx = SSLContext.getInstance("TLS")
          ctx.init(null, Array[TrustManager](TrustAllCertificates), null)
          ctx
        }
      val sslEngine         = sslContext.createSSLEngine()
      val params            = sslEngine.getSSLParameters
      sni.foreach(sni => params.setServerNames(Arrays.asList(new SNIHostName(sni))))
      if (verifyCertificate) {
        params.setEndpointIdentificationAlgorithm("HTTPS")
      }
      sslEngine.setUseClientMode(true)
      sslEngine.setSSLParameters(params)
      val selector          = Selector.open()
      val rawChannel        = SocketChannel.open()
      rawChannel.configureBlocking(false)
      rawChannel.connect(address)
      rawChannel.register(selector, SelectionKey.OP_CONNECT)
      val tlsChannelBuilder = ClientTlsChannel.newBuilder(rawChannel, sslEngine)
      val tlsChannel        = tlsChannelBuilder.build()
      selector.select()
      rawChannel.finishConnect()
      rawChannel.register(selector, SelectionKey.OP_WRITE)
      val channelGroup      = new AsynchronousTlsChannelGroup()
      new AsynchronousTlsChannel(channelGroup, tlsChannel, rawChannel)
    }

  private object TrustAllCertificates extends X509TrustManager {
    override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = ()
    override def getAcceptedIssuers: Array[X509Certificate]                                = Array.empty
  }
}
