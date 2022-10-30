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

package zio.redis.executor.cluster

import zio.redis._
import zio.redis.api.Cluster.AskingCommand
import zio.redis.codec.StringUtf8Codec
import zio.redis.executor.cluster.RedisClusterExecutorLive._
import zio.redis.executor.node.RedisNodeExecutorLive
import zio.redis.executor.{RedisConnectionLive, RedisExecutor}
import zio.redis.options.Cluster._
import zio.schema.codec.Codec
import zio.{Chunk, Exit, IO, Ref, Schedule, Scope, UIO, ZIO, ZLayer, durationInt}

import java.io.IOException
import scala.util.Try

private[redis] final case class RedisClusterExecutorLive(
  clusterConnectionRef: Ref.Synchronized[ClusterConnection],
  scope: Scope.Closeable
) extends RedisExecutor {

  def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] = {

    def executeAsk(executorZIO: IO[RedisError.IOError, RedisExecutor], asking: Boolean = false) =
      for {
        executor <- executorZIO
        _        <- ZIO.when(asking)(executor.execute(AskingCommand.resp((), StringUtf8Codec)))
        res      <- executor.execute(command)
      } yield res

    def executeSafe(keySlot: Slot) = {
      val recover = executeAsk(getExecutor(keySlot)).flatMap {
        case e: RespValue.Error => ZIO.fail(e.toRedisError)
        case success            => ZIO.succeed(success)
      }.catchSome {
        case e: RedisError.Ask   => executeAsk(getExecutor(e.address), asking = true)
        case _: RedisError.Moved => refreshConnect() *> executeAsk(getExecutor(keySlot))
      }
      recover.retry(Scheduler)
    }

    for {
      key    <- Try(command(1)).fold(_ => ZIO.fail(CusterKeyError), bs => ZIO.succeedNow(bs))
      keySlot = Slot((key.asCRC16 % SlotsAmount).toLong)
      result <- executeSafe(keySlot)
    } yield result
  }

  private def getExecutor(slot: Slot): UIO[RedisExecutor] =
    clusterConnectionRef.get.map(cc => cc.executor(slot))

  // TODO introduce max connection amount
  private def getExecutor(address: RedisUri) =
    clusterConnectionRef.modifyZIO { cc =>
      val executorOpt       = cc.executors.get(address).map(es => (es.executor, cc))
      val enrichedClusterIO = scope.extend(connectToNode(address)).map(es => (es.executor, cc.addExecutor(address, es)))
      ZIO.fromOption(executorOpt).catchAll(_ => enrichedClusterIO)
    }

  private def refreshConnect(): ZIO[Any, RedisError, Unit] =
    clusterConnectionRef.updateZIO { connection =>
      val addresses = connection.partitions.flatMap(_.addresses)
      for {
        cluster <- scope.extend(initConnectToCluster(addresses))
        _       <- ZIO.foreachParDiscard(connection.executors) { case (_, es) => es.scope.close(Exit.unit) }
      } yield cluster
    }

  // TODO configurable
  private val Scheduler: Schedule[Any, Throwable, (zio.Duration, Long, Throwable)] =
    Schedule.exponential(100.millis, 1.5) && Schedule.recurs(5) && Schedule.recurWhile[Throwable] {
      case _: RedisError.IOError | _: RedisError.ClusterRedisError => true
      case _                                                       => false
    }

}

object RedisClusterExecutorLive {

  lazy val layer: ZLayer[RedisClusterConfig, RedisError, RedisExecutor] = ZLayer.scoped {
    for {
      config       <- ZIO.service[RedisClusterConfig]
      layerScope   <- ZIO.scope
      clusterScope <- Scope.make
      executor     <- clusterScope.extend(create(config, clusterScope))
      _            <- layerScope.addFinalizerExit(e => clusterScope.close(e))
    } yield executor
  }

  private[redis] def create(
    config: RedisClusterConfig,
    scope: Scope.Closeable
  ): ZIO[Scope, RedisError, RedisClusterExecutorLive] =
    for {
      clusterConnection    <- initConnectToCluster(config.addresses)
      clusterConnectionRef <- Ref.Synchronized.make(clusterConnection)
      clusterExec           = RedisClusterExecutorLive(clusterConnectionRef, scope)
      _                    <- logScopeFinalizer("Cluster executor is closed")
    } yield clusterExec

  private def initConnectToCluster(
    addresses: Chunk[RedisUri]
  ): ZIO[Scope, RedisError, ClusterConnection] =
    ZIO
      .collectFirst(addresses) { address =>
        connectToCluster(address).foldZIO(
          error => ZIO.logError(s"The connection to cluster has been failed, $error") *> ZIO.succeedNow(None),
          cc => ZIO.logInfo("The connection to cluster has been established") *> ZIO.succeedNow(Some(cc))
        )
      }
      .flatMap(cc => ZIO.getOrFailWith(CusterConnectionError)(cc))

  private def connectToCluster(address: RedisUri) =
    for {
      temporaryRedis    <- getRedis(address)
      (trLayer, trScope) = temporaryRedis
      partitions        <- slots().provideLayer(trLayer)
      _                 <- ZIO.logTrace(s"Cluster configs:\n${partitions.mkString("\n")}")
      uniqueAddresses    = partitions.map(_.master.address).distinct
      uriExecScope      <- ZIO.foreachPar(uniqueAddresses)(address => connectToNode(address).map(es => address -> es))
      slots              = getSlotAddress(partitions)
      _                 <- trScope.close(Exit.unit)
    } yield ClusterConnection(partitions, uriExecScope.toMap, slots)

  private def connectToNode(address: RedisUri) =
    for {
      closableScope <- Scope.make
      connection    <- closableScope.extend(RedisConnectionLive.create(address))
      executor      <- closableScope.extend(RedisNodeExecutorLive.create(connection))
      layerScope    <- ZIO.scope
      _             <- layerScope.addFinalizerExit(e => closableScope.close(e))
    } yield ExecutorScope(executor, closableScope)

  private def getRedis(address: RedisUri) = {
    val codec    = ZLayer.succeed[Codec](StringUtf8Codec)
    val executor = ZLayer.succeed(address) >>> RedisExecutor.layer
    val redis    = executor ++ codec >>> RedisLive.layer
    for {
      closableScope <- Scope.make
      layer         <- closableScope.extend(redis.memoize)
      _             <- logScopeFinalizer("Temporary redis connection is closed")
    } yield (layer, closableScope)
  }

  private def getSlotAddress(partitions: Chunk[Partition]) =
    partitions.flatMap { p =>
      for (i <- p.slotRange.start to p.slotRange.end) yield Slot(i) -> p.master.address
    }.toMap

  private val CusterKeyError =
    RedisError.ProtocolError("Key doesn't found. No way to dispatch this command to Redis Cluster")
  private val CusterConnectionError =
    RedisError.IOError(new IOException("The connection to cluster has been failed. Can't reach a single startup node."))
}
