package zio.redis.executor.cluster

import zio.redis._
import zio.redis.codec.CRC16
import zio.redis.executor.RedisExecutor
import zio.redis.options.Cluster.{Slot, SlotsAmount}
import zio.test._
import zio.{Chunk, Layer, ZIO, ZLayer, durationInt}

object RedisClusterExecutorLiveSpec extends BaseSpec {
  def spec: Spec[TestEnvironment, Any] =
    suite("cluster executor")(
      test("check cluster responsiveness when ASK redirect happens") {
        for {
          initSlots       <- slots
          key             <- uuid
          value1          <- get(key).returning[String]
          keySlot          = Slot(CRC16.get(Chunk.fromArray(key.getBytes)).toLong % SlotsAmount)
          (sourcePart, id) = initSlots.zipWithIndex.find { case (p, _) => p.slotRange.isContain(keySlot) }.get
          sourceMaster     = sourcePart.master
          destPart         = initSlots((id + 1) % initSlots.size)
          destMaster       = destPart.master
          destMasterConn   = getRedisNodeLayer(destMaster.address)
          _                = ZIO.logDebug(s"$key _____ Importing $keySlot to ${destMaster.id} - ${destMaster.address}")
          _               <- setSlotImporting(keySlot, sourceMaster.id).provideLayer(destMasterConn)
          _                = ZIO.logDebug(s"$key _____ Migrating $keySlot from ${sourceMaster.id}- ${sourceMaster.address}")
          sourceMasterConn = getRedisNodeLayer(sourceMaster.address)
          _               <- setSlotMigrating(keySlot, destMaster.id).provideLayer(sourceMasterConn)
          value2          <- get(key).returning[String] // have to redirect without error ASK
          value3          <- get(key).returning[String] // have to redirect without creating new connection
          _               <- setSlotStable(keySlot).provideLayer(destMasterConn)
        } yield {
          assertTrue(value1 == value2) && assertTrue(value2 == value3)
        }
      },
      test("check client responsiveness when Moved redirect happened") {
        for {
          initSlots       <- slots
          key             <- uuid
          _               <- set(key, "value")
          value1          <- get(key).returning[String]
          keySlot          = Slot(CRC16.get(Chunk.fromArray(key.getBytes)).toLong % SlotsAmount)
          (sourcePart, id) = initSlots.zipWithIndex.find { case (p, _) => p.slotRange.isContain(keySlot) }.get
          sourceMaster     = sourcePart.master
          destPart         = initSlots((id + 1) % initSlots.size)
          destMaster       = destPart.master
          destMasterConn   = getRedisNodeLayer(destMaster.address)
          _               <- ZIO.logDebug(s"$key _____ Importing $keySlot to ${destMaster.id}")
          _               <- setSlotImporting(keySlot, sourceMaster.id).provideLayer(destMasterConn)
          _               <- ZIO.logDebug(s"$key _____ Migrating $keySlot from ${sourceMaster.id}")
          sourceMasterConn = getRedisNodeLayer(sourceMaster.address)
          _               <- setSlotMigrating(keySlot, destMaster.id).provideLayer(sourceMasterConn)
          _ <- migrate(destMaster.address.host, destMaster.address.port.toLong, key, 0, 5.seconds, keys = None)
                 .provideLayer(sourceMasterConn)
          _      <- setSlotNode(keySlot, destMaster.id).provideLayer(destMasterConn)
          _      <- setSlotNode(keySlot, destMaster.id).provideLayer(sourceMasterConn)
          value2 <- get(key).returning[String] // have to refresh connection
          value3 <- get(key).returning[String] // have to get value without refreshing connection
        } yield {
          assertTrue(value1 == value2) && assertTrue(value2 == value3)
        }
      }
    ).provideLayerShared(ClusterLayer)

  private final def getRedisNodeLayer(uri: RedisUri): Layer[Any, Redis] =
    ZLayer.make[Redis](
      ZLayer.succeed(RedisConfig(uri.host, uri.port)),
      RedisExecutor.layer,
      ZLayer.succeed(codec),
      RedisLive.layer
    )

  private val ClusterLayer: Layer[Any, Redis] = {
    val address1 = RedisUri("localhost", 5010)
    val address2 = RedisUri("localhost", 5000)
    ZLayer.make[Redis](
      ZLayer.succeed(RedisClusterConfig(Chunk(address1, address2))),
      RedisClusterExecutorLive.layer.orDie,
      ZLayer.succeed(codec),
      RedisLive.layer
    )
  }
}
