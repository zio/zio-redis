package zio.redis.internal

import zio._
import zio.redis._
import zio.redis.internal.CRC16
import zio.redis.options.Cluster.{Slot, SlotsAmount}
import zio.test._

object ClusterExecutorSpec extends BaseSpec {
  def spec: Spec[TestEnvironment, Any] =
    suite("cluster executor")(
      test("check cluster responsiveness when ASK redirect happens") {
        for {
          redis           <- ZIO.service[Redis]
          initSlots       <- redis.slots
          key             <- uuid
          value1          <- redis.get(key).returning[String]
          keySlot          = Slot(CRC16.get(Chunk.fromArray(key.getBytes)).toLong % SlotsAmount)
          (sourcePart, id) = initSlots.zipWithIndex.find { case (p, _) => p.slotRange.contains(keySlot) }.get
          sourceMaster     = sourcePart.master
          destPart         = initSlots((id + 1) % initSlots.size)
          destMaster       = destPart.master
          destMasterConn   = getRedisNodeLayer(destMaster.address)
          _                = ZIO.logDebug(s"$key _____ Importing $keySlot to ${destMaster.id} - ${destMaster.address}")
          _               <- ZIO.serviceWithZIO[Redis](_.setSlotImporting(keySlot, sourceMaster.id)).provideLayer(destMasterConn)
          _                = ZIO.logDebug(s"$key _____ Migrating $keySlot from ${sourceMaster.id}- ${sourceMaster.address}")
          sourceMasterConn = getRedisNodeLayer(sourceMaster.address)
          _               <- ZIO.serviceWithZIO[Redis](_.setSlotMigrating(keySlot, destMaster.id)).provideLayer(sourceMasterConn)
          value2          <- redis.get(key).returning[String] // have to redirect without error ASK
          value3          <- redis.get(key).returning[String] // have to redirect without creating new connection
          _               <- ZIO.serviceWithZIO[Redis](_.setSlotStable(keySlot)).provideLayer(destMasterConn)
        } yield assertTrue(value1 == value2) && assertTrue(value2 == value3)
      } @@ TestAspect.flaky,
      test("check client responsiveness when Moved redirect happened") {
        for {
          redis           <- ZIO.service[Redis]
          initSlots       <- redis.slots
          key             <- uuid
          _               <- redis.set(key, "value")
          value1          <- redis.get(key).returning[String]
          keySlot          = Slot(CRC16.get(Chunk.fromArray(key.getBytes)).toLong % SlotsAmount)
          (sourcePart, id) = initSlots.zipWithIndex.find { case (p, _) => p.slotRange.contains(keySlot) }.get
          sourceMaster     = sourcePart.master
          destPart         = initSlots((id + 1) % initSlots.size)
          destMaster       = destPart.master
          destMasterConn   = getRedisNodeLayer(destMaster.address)
          _               <- ZIO.logDebug(s"$key _____ Importing $keySlot to ${destMaster.id}")
          _               <- ZIO.serviceWithZIO[Redis](_.setSlotImporting(keySlot, sourceMaster.id)).provideLayer(destMasterConn)
          _               <- ZIO.logDebug(s"$key _____ Migrating $keySlot from ${sourceMaster.id}")
          sourceMasterConn = getRedisNodeLayer(sourceMaster.address)
          _               <- ZIO.serviceWithZIO[Redis](_.setSlotMigrating(keySlot, destMaster.id)).provideLayer(sourceMasterConn)
          _ <- ZIO
                 .serviceWithZIO[Redis](
                   _.migrate(destMaster.address.host, destMaster.address.port.toLong, key, 0, 5.seconds, keys = None)
                 )
                 .provideLayer(sourceMasterConn)
          _      <- ZIO.serviceWithZIO[Redis](_.setSlotNode(keySlot, destMaster.id)).provideLayer(destMasterConn)
          _      <- ZIO.serviceWithZIO[Redis](_.setSlotNode(keySlot, destMaster.id)).provideLayer(sourceMasterConn)
          value2 <- redis.get(key).returning[String] // have to refresh connection
          value3 <- redis.get(key).returning[String] // have to get value without refreshing connection
        } yield assertTrue(value1 == value2) && assertTrue(value2 == value3)
      }
    ).provideLayerShared(ClusterLayer)

  private final def getRedisNodeLayer(uri: RedisUri): Layer[Any, Redis] =
    ZLayer.make[Redis](
      ZLayer.succeed(RedisConfig(uri.host, uri.port)),
      ZLayer.succeed(ProtobufCodecSupplier),
      Redis.singleNode
    )

  private val ClusterLayer: Layer[Any, Redis] = {
    val address1 = RedisUri("localhost", 5010)
    val address2 = RedisUri("localhost", 5000)

    ZLayer.make[Redis](
      ZLayer.succeed(RedisClusterConfig(Chunk(address1, address2))),
      ZLayer.succeed(ProtobufCodecSupplier),
      Redis.cluster
    )
  }
}
