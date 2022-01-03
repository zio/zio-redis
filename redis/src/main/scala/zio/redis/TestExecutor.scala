package zio.redis

import java.nio.file.{FileSystems, Paths}
import java.time.Instant

import zio._
import zio.clock.Clock
import zio.duration._
import zio.redis.RedisError.ProtocolError
import zio.redis.RespValue.{bulkString, BulkString}
import zio.redis.codec.StringUtf8Codec
import zio.redis.TestExecutor.{KeyInfo, KeyType}
import zio.schema.codec.Codec
import zio.stm.{random => _, _}

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList
import scala.util.Try

private[redis] final class TestExecutor private (
  clientInfo: TRef[ClientInfo],
  clientTrackingInfo: TRef[ClientTrackingInfo],
  keys: TMap[String, KeyInfo],
  lists: TMap[String, Chunk[String]],
  sets: TMap[String, Set[String]],
  strings: TMap[String, String],
  randomPick: Int => USTM[Int],
  hyperLogLogs: TMap[String, Set[String]],
  hashes: TMap[String, Map[String, String]],
  sortedSets: TMap[String, Map[String, Double]],
  clock: Clock
) extends RedisExecutor.Service {

  override val codec: Codec = StringUtf8Codec

  override def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
    for {
      name <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
      now  <- clock.get.instant
      _    <- clearExpired(now).commit
      result <- name.asString match {
                  case api.Lists.BlPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray, now)

                  case api.Lists.BrPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray, now)

                  case api.Lists.BrPopLPush =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)

                  case api.Lists.BlMove =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)

                  case api.SortedSets.BzPopMax =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)

                  case api.SortedSets.BzPopMin =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)

                  case "CLIENT" =>
                    val command1 = command.tail
                    val name1    = command1.head
                    runCommand(name.asString + " " + name1.asString, command1.tail, now).commit

                  case _ => runCommand(name.asString, command.tail, now).commit
                }
    } yield result

  private def runBlockingCommand(
    name: String,
    input: Chunk[RespValue.BulkString],
    timeout: Int,
    respValue: RespValue,
    now: Instant
  ): UIO[RespValue] =
    if (timeout > 0) {
      runCommand(name, input, now).commit
        .timeout(timeout.seconds)
        .map(_.getOrElse(respValue))
        .provideLayer(Clock.live)
    } else
      runCommand(name, input, now).commit

  private def errResponse(cmd: String): RespValue.BulkString =
    RespValue.bulkString(s"(error) ERR wrong number of arguments for '$cmd' command")

  private def onConnection(command: String, input: Chunk[RespValue.BulkString])(
    res: => RespValue.BulkString
  ): USTM[BulkString] = STM.succeedNow(if (input.isEmpty) errResponse(command) else res)

  private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString], now: Instant): USTM[RespValue] = {
    name match {
      case api.Connection.Auth =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Connection.ClientCaching =>
        val yesNoOption = input.headOption.map(_.asString)
        orMissingParameter(yesNoOption) { yesNo =>
          orInvalidParameter(STM.succeed(yesNo == "YES" || yesNo == "NO"))(
            clientTrackingInfo.get.flatMap { trackingInfo =>
              val flags = trackingInfo.flags
              if (flags.clientSideCaching && yesNo == "YES" && flags.trackingMode.contains(ClientTrackingMode.OptIn))
                clientTrackingInfo
                  .set(trackingInfo.copy(flags = flags.copy(caching = Some(true))))
                  .map(_ => RespValue.SimpleString("OK"))
              else if (flags.clientSideCaching && flags.trackingMode.contains(ClientTrackingMode.OptOut))
                clientTrackingInfo
                  .set(trackingInfo.copy(flags = flags.copy(caching = Some(false))))
                  .map(_ => RespValue.SimpleString("OK"))
              else
                STM.succeed(
                  RespValue.Error(
                    "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled"
                  )
                )
            }
          )
        }

      case api.Connection.ClientId =>
        clientInfo.get.map(info => RespValue.Integer(info.id))

      case api.Connection.ClientKill =>
        if (input.length == 1) {
          val addressOption = input.headOption.map(_.asString)

          orMissingParameter(addressOption)(address =>
            clientInfo.get.map(info =>
              if (info.address.fold(false)(_.stringify == address)) RespValue.SimpleString("OK")
              else RespValue.Error("ERR")
            )
          )
        } else {
          val inputList = input.toList.map(_.asString).grouped(2)
          val notSkipMe = inputList.contains(List("SKIPME", "NO"))
          if (notSkipMe) {
            inputList.map {
              case List("ADDR", addr) =>
                clientInfo.get.map(_.address.fold(false)(_.stringify == addr))
              case List("LADDR", laddr) =>
                clientInfo.get.map(_.localAddress.fold(false)(_.stringify == laddr))
              case List("ID", id) =>
                clientInfo.get.map(_.id.toString == id)
              case List("TYPE", clientType) =>
                clientInfo.get.map(info =>
                  clientType match {
                    case string if string == ClientType.PubSub.stringify  => info.flags.contains(ClientFlag.PubSub)
                    case string if string == ClientType.Master.stringify  => info.flags.contains(ClientFlag.IsMaster)
                    case string if string == ClientType.Replica.stringify => info.flags.contains(ClientFlag.Replica)
                    case string if string == ClientType.Normal.stringify =>
                      !info.flags.contains(ClientFlag.PubSub) &&
                      !info.flags.contains(ClientFlag.IsMaster) && !info.flags.contains(ClientFlag.Replica)
                    case _ => false
                  }
                )
              case List("USER", user) =>
                clientInfo.get.map(_.user.fold(false)(_ == user))
              case _ => STM.succeed(true)
            }.fold(STM.succeed(true))((a, b) => a.flatMap(x => b.map(y => x && y)))
              .map(bool => RespValue.Integer(if (bool) 1 else 0))
          } else STM.succeedNow(RespValue.Integer(0))
        }

      case api.Connection.ClientGetName =>
        clientInfo.get.map(_.name.fold[RespValue](RespValue.NullBulkString)(name => RespValue.bulkString(name)))

      case api.Connection.ClientGetRedir =>
        clientTrackingInfo.get.map { trackingInfo =>
          val redirect = trackingInfo.redirect match {
            case ClientTrackingRedirect.RedirectedTo(id) => id
            case ClientTrackingRedirect.NotRedirected    => 0L
            case ClientTrackingRedirect.NotEnabled       => -1L
          }

          RespValue.Integer(redirect)
        }

      case api.Connection.ClientUnpause =>
        STM.succeedNow(RespValue.SimpleString("OK"))

      case api.Connection.ClientPause =>
        STM.succeedNow(RespValue.SimpleString("OK"))

      case api.Connection.ClientSetName =>
        val nameOption = input.headOption.map(_.asString)

        orMissingParameter(nameOption)(name =>
          clientInfo.getAndUpdate(_.copy(name = Some(name))).as(RespValue.SimpleString("OK"))
        )

      case api.Connection.ClientTracking =>
        val inputList   = input.toList.map(_.asString)
        val onOffOption = inputList.headOption

        orMissingParameter(onOffOption)(onOff =>
          orInvalidParameter(STM.succeed(onOff == "ON" || onOff == "OFF"))(
            if (onOff == "ON") {
              val mode = inputList match {
                case list if list.contains(ClientTrackingMode.OptIn.stringify)     => Some(ClientTrackingMode.OptIn)
                case list if list.contains(ClientTrackingMode.OptOut.stringify)    => Some(ClientTrackingMode.OptOut)
                case list if list.contains(ClientTrackingMode.Broadcast.stringify) => Some(ClientTrackingMode.Broadcast)
                case _                                                             => None
              }
              val noLoop = inputList.contains("NOLOOP")
              val filteredInput = inputList
                .drop(1)
                .filterNot(
                  Set(
                    ClientTrackingMode.OptIn.stringify,
                    ClientTrackingMode.OptOut.stringify,
                    ClientTrackingMode.Broadcast.stringify,
                    "NOLOOP"
                  )
                )
                .grouped(2)
                .toList
              val redirectId = filteredInput.filter(_.head == "REDIRECT").map(_(1).toLong).headOption
              val prefixes = Some(filteredInput.filter(_.head == "PREFIX").map(_(1)).toSet)
                .map(set => if (set.isEmpty && mode.contains(ClientTrackingMode.Broadcast)) Set("") else set)
                .filter(_.nonEmpty)

              clientTrackingInfo.get.flatMap { trackingInfo =>
                import ClientTrackingMode._

                val trackingMode = trackingInfo.flags.trackingMode

                if (prefixes.nonEmpty && !mode.contains(Broadcast))
                  STM.succeed(
                    RespValue.Error("ERR PREFIX option requires BCAST mode to be enabled")
                  )
                else if (
                  trackingMode.contains(Broadcast) && !mode.contains(Broadcast) || mode.contains(Broadcast) &&
                  trackingMode.fold(false)(_ != Broadcast)
                )
                  STM.succeed(
                    RespValue.Error(
                      "You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode"
                    )
                  )
                else if (
                  mode.contains(OptIn) && trackingMode.contains(OptOut) || mode.contains(OptOut) && trackingMode
                    .contains(OptIn)
                )
                  STM.succeed {
                    RespValue.Error(
                      "ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode"
                    )
                  }
                else {
                  val addTracking = for {
                    _ <- clientInfo.getAndUpdate(info => info.copy(flags = info.flags + ClientFlag.KeysTrackingEnabled))
                    _ <- clientTrackingInfo.getAndUpdate(_.copy(flags = ClientTrackingFlags(clientSideCaching = true)))
                  } yield ()

                  val addMode = mode.fold(STM.unit)(mode =>
                    for {
                      _ <- clientTrackingInfo.getAndUpdate(trackingInfo =>
                             trackingInfo.copy(flags = trackingInfo.flags.copy(trackingMode = Some(mode)))
                           )
                      _ <- STM.when(mode == ClientTrackingMode.Broadcast)(
                             clientInfo.getAndUpdate(info =>
                               info.copy(flags = info.flags + ClientFlag.BroadcastTrackingMode)
                             )
                           )
                    } yield ()
                  )

                  val addNoLoop =
                    clientTrackingInfo.getAndUpdate(trackingInfo =>
                      trackingInfo.copy(flags = trackingInfo.flags.copy(noLoop = noLoop))
                    )

                  val addRedirectId = for {
                    _ <- clientInfo.getAndUpdate(_.copy(redirectionClientId = redirectId))
                    _ <- clientTrackingInfo.getAndUpdate(
                           _.copy(redirect =
                             redirectId.fold[ClientTrackingRedirect](ClientTrackingRedirect.NotRedirected)(id =>
                               ClientTrackingRedirect.RedirectedTo(id)
                             )
                           )
                         )
                  } yield ()

                  val addPrefixes = prefixes.fold(STM.unit)(prefixes =>
                    clientTrackingInfo
                      .getAndUpdate(trackingInfo => trackingInfo.copy(prefixes = trackingInfo.prefixes ++ prefixes))
                      .as(())
                  )

                  addTracking *> addMode *> addNoLoop *> addRedirectId *> addPrefixes.as(RespValue.SimpleString("OK"))
                }
              }
            } else
              for {
                _ <-
                  clientInfo.getAndUpdate(info =>
                    info.copy(flags =
                      info.flags -- Set(ClientFlag.KeysTrackingEnabled, ClientFlag.BroadcastTrackingMode)
                    )
                  )
                _ <- clientTrackingInfo.set(
                       ClientTrackingInfo(
                         ClientTrackingFlags(clientSideCaching = false),
                         ClientTrackingRedirect.NotEnabled
                       )
                     )
              } yield RespValue.SimpleString("OK")
          )
        )

      case api.Connection.ClientTrackingInfo =>
        clientTrackingInfo.get.map { trackingInfo =>
          val flags = Chunk.single(if (trackingInfo.flags.clientSideCaching) "on" else "off") ++
            trackingInfo.flags.trackingMode.fold[Chunk[String]](Chunk.empty)(mode =>
              Chunk.single(mode.stringify.toLowerCase)
            ) ++
            (if (trackingInfo.flags.noLoop) Chunk.single("noloop") else Chunk.empty) ++
            trackingInfo.flags.caching
              .fold[Chunk[String]](Chunk.empty)(condition =>
                Chunk.single(if (condition) "caching-yes" else "caching-no")
              ) ++
            (if (trackingInfo.flags.brokenRedirect) Chunk.single("broken_redirect") else Chunk.empty)
          val redirect = trackingInfo.redirect match {
            case ClientTrackingRedirect.RedirectedTo(id) => id
            case ClientTrackingRedirect.NotRedirected    => 0L
            case ClientTrackingRedirect.NotEnabled       => -1L
          }
          val respPrefixes =
            if (trackingInfo.prefixes.isEmpty) RespValue.NullArray
            else RespValue.array(trackingInfo.prefixes.toSeq.map(RespValue.bulkString): _*)

          RespValue.array(
            RespValue.bulkString("flags"),
            RespValue.Array(flags.map(RespValue.bulkString)),
            RespValue.bulkString("redirect"),
            RespValue.Integer(redirect),
            RespValue.bulkString("prefixes"),
            respPrefixes
          )
        }

      case api.Connection.ClientUnblock =>
        STM.succeedNow(RespValue.Integer(0))

      case api.Connection.Echo =>
        val messageOption = input.headOption

        orMissingParameter(messageOption)(message => onConnection(name, input)(message))

      case api.Connection.Ping =>
        STM.succeedNow {
          if (input.isEmpty)
            RespValue.bulkString("PONG")
          else
            input.head
        }

      case api.Connection.Quit =>
        STM.succeedNow(RespValue.SimpleString("OK"))

      case api.Connection.Reset =>
        STM.succeedNow(RespValue.SimpleString("RESET"))

      case api.Connection.Select =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Geo.GeoAdd =>
        val keyOption = input.headOption.map(_.asString)
        val valuesOption = NonEmptyChunk.fromChunk(
          Chunk.fromIterator(
            input
              .drop(1)
              .map(_.asString)
              .grouped(3)
              .map(g => (LongLat(g(0).toDouble, g(1).toDouble), g(2)))
          )
        )

        orMissingParameter2(keyOption, valuesOption) { (key, values) =>
          orWrongType(isSortedSet(key))(
            orInvalidParameter(STM.succeed(values.map(_._1).forall(Hash.isValidLongLat))) {
              val members = values.map { case (longLat, member) =>
                MemberScore(Hash.encodeAsHash(longLat.longitude, longLat.latitude).toDouble, member)
              }

              for {
                scoreMap    <- sortedSets.getOrElse(key, Map.empty)
                membersAdded = members.count(ms => scoreMap.contains(ms.member))
                newScoreMap  = scoreMap ++ members.map(ms => (ms.member, ms.score)).toMap
                _           <- putSortedSet(key, newScoreMap)
              } yield RespValue.Integer(membersAdded.toLong)
            }
          )
        }

      case api.Geo.GeoDist =>
        val stringInput = input.map(_.asString)

        val keyOption     = stringInput.headOption
        val member1Option = stringInput.lift(1)
        val member2Option = stringInput.lift(2)
        val unit = stringInput.lift(3).flatMap {
          case "m"  => Some(RadiusUnit.Meters)
          case "km" => Some(RadiusUnit.Kilometers)
          case "ft" => Some(RadiusUnit.Feet)
          case "mi" => Some(RadiusUnit.Miles)
          case _    => None
        }

        orMissingParameter3(keyOption, member1Option, member2Option) { (key, member1, member2) =>
          orWrongType(isSortedSet(key))(
            for {
              scoreMap <- sortedSets.getOrElse(key, Map.empty)
              hashes    = Chunk(member1, member2).collect(scoreMap)
              distOption = if (hashes.size == 2)
                             Some(
                               Hash
                                 .distance(
                                   Hash.decodeHash(hashes(0).toLong),
                                   Hash.decodeHash(hashes(1).toLong),
                                   unit.getOrElse(RadiusUnit.Meters)
                                 )
                             )
                           else None
            } yield distOption.fold[RespValue](RespValue.NullBulkString)(dist => RespValue.bulkString(dist.toString))
          )
        }

      case api.Geo.GeoHash =>
        val keyOption     = input.headOption.map(_.asString)
        val membersOption = NonEmptyChunk.fromChunk(input.drop(1).map(_.asString))

        orMissingParameter2(keyOption, membersOption) { (key, members) =>
          orWrongType(isSortedSet(key))(
            for {
              scoreMap <- sortedSets.getOrElse(key, Map.empty)
              respChunk = members.map(scoreMap.get(_).fold[RespValue](RespValue.NullBulkString) { hash =>
                            val geoHash = Hash.asGeoHash(hash.toLong)
                            RespValue.bulkString(geoHash)
                          })
            } yield RespValue.Array(respChunk)
          )
        }

      case api.Geo.GeoPos =>
        val keyOption     = input.headOption.map(_.asString)
        val membersOption = NonEmptyChunk.fromChunk(input.drop(1).map(_.asString))

        orMissingParameter2(keyOption, membersOption) { (key, members) =>
          orWrongType(isSortedSet(key))(
            for {
              scoreMap <- sortedSets.getOrElse(key, Map.empty)
              respChunk = members.map(scoreMap.get(_).fold[RespValue](RespValue.NullArray) { hash =>
                            val longLat = Hash.decodeHash(hash.toLong)
                            RespValue.array(
                              RespValue.bulkString(longLat.longitude.toString),
                              RespValue.bulkString(longLat.latitude.toString)
                            )
                          })
            } yield RespValue.Array(respChunk)
          )
        }

      case api.Geo.GeoRadius =>
        val stringInput = input.map(_.asString)

        val keyOption = stringInput.headOption
        val centerOption = for {
          longitude <- stringInput.lift(1).map(_.toDouble)
          latitude  <- stringInput.lift(2).map(_.toDouble)
        } yield LongLat(longitude, latitude)
        val radiusOption = stringInput.lift(3).map(_.toDouble)
        val unitOption = stringInput.lift(4).flatMap {
          case "m"  => Some(RadiusUnit.Meters)
          case "km" => Some(RadiusUnit.Kilometers)
          case "ft" => Some(RadiusUnit.Feet)
          case "mi" => Some(RadiusUnit.Miles)
          case _    => None
        }
        val withCoord = stringInput.find(_ == WithCoord.stringify)
        val withDist  = stringInput.find(_ == WithDist.stringify)
        val withHash  = stringInput.find(_ == WithHash.stringify)
        val count = stringInput.toList.sliding(2).collectFirst { case "COUNT" :: count :: _ =>
          count.toInt
        }
        val order = stringInput.collectFirst {
          case "ASC"  => Order.Ascending
          case "DESC" => Order.Descending
        }
        val store = stringInput.toList.sliding(2).collectFirst { case "STORE" :: key :: _ =>
          key
        }
        val storeDist = stringInput.toList.sliding(2).collectFirst { case "STOREDIST" :: key :: _ =>
          key
        }

        orMissingParameter4(keyOption, centerOption, radiusOption, unitOption) { (key, center, radius, unit) =>
          orWrongType(isSortedSet(key))(
            {
              for {
                scoreMap <- sortedSets.getOrElse(key, Map.empty)
                list = scoreMap
                         .map(x => x -> Hash.distance(center, Hash.decodeHash(x._2.toLong), unit))
                         .filter(_._2 <= radius)
                         .toList
                orderedList = order.fold(list) {
                                case Order.Ascending  => list.sortWith(_._2 < _._2)
                                case Order.Descending => list.sortWith(_._2 > _._2)
                              }
                countedList = count.fold(orderedList)(orderedList.take)
                _ <- store.fold(STM.unit)(key =>
                       putSortedSet(
                         key,
                         countedList.map { case ((name, score), _) =>
                           (name, score)
                         }.toMap
                       )
                     )
                _ <- storeDist.fold(STM.unit)(key =>
                       putSortedSet(
                         key,
                         countedList.map { case ((name, _), distance) =>
                           (name, distance)
                         }.toMap
                       )
                     )
                chunk =
                  Chunk.fromIterable(
                    countedList.map { case ((name, score), distance) =>
                      val nameResp = RespValue.bulkString(name)
                      val infoChunk =
                        withCoord.fold[Chunk[RespValue]](Chunk.empty) { _ =>
                          val longLat = Hash.decodeHash(score.toLong)

                          Chunk.single(
                            RespValue.array(
                              RespValue.bulkString(longLat.longitude.toString),
                              RespValue.bulkString(longLat.latitude.toString)
                            )
                          )
                        } ++
                          withDist
                            .fold[Chunk[RespValue]](Chunk.empty)(_ =>
                              Chunk.single(RespValue.bulkString(distance.toString))
                            ) ++
                          withHash
                            .fold[Chunk[RespValue]](Chunk.empty)(_ => Chunk.single(RespValue.Integer(score.toLong)))

                      if (infoChunk.isEmpty) nameResp else RespValue.Array(nameResp +: infoChunk)
                    }
                  )
              } yield
                if (store.nonEmpty || storeDist.nonEmpty) RespValue.Integer(countedList.size.toLong)
                else if (chunk.nonEmpty) RespValue.Array(chunk)
                else RespValue.NullArray
            }
          )
        }

      case api.Geo.GeoRadiusByMember =>
        val stringInput = input.map(_.asString)

        val keyOption    = stringInput.headOption
        val memberOption = stringInput.lift(1)
        val radiusOption = stringInput.lift(2).map(_.toDouble)
        val unitOption = stringInput.lift(3).flatMap {
          case "m"  => Some(RadiusUnit.Meters)
          case "km" => Some(RadiusUnit.Kilometers)
          case "ft" => Some(RadiusUnit.Feet)
          case "mi" => Some(RadiusUnit.Miles)
          case _    => None
        }
        val withCoord = stringInput.find(_ == WithCoord.stringify)
        val withDist  = stringInput.find(_ == WithDist.stringify)
        val withHash  = stringInput.find(_ == WithHash.stringify)
        val count = stringInput.toList.sliding(2).collectFirst { case "COUNT" :: count :: _ =>
          count.toInt
        }
        val order = stringInput.collectFirst {
          case "ASC"  => Order.Ascending
          case "DESC" => Order.Descending
        }
        val store = stringInput.toList.sliding(2).collectFirst { case "STORE" :: key :: _ =>
          key
        }
        val storeDist = stringInput.toList.sliding(2).collectFirst { case "STOREDIST" :: key :: _ =>
          key
        }

        orMissingParameter4(keyOption, memberOption, radiusOption, unitOption) { (key, member, radius, unit) =>
          orWrongType(isSortedSet(key))(
            {
              sortedSets.getOrElse(key, Map.empty).flatMap { scoreMap =>
                if (scoreMap.contains(member)) {
                  val center = scoreMap(member).toLong
                  val list =
                    scoreMap
                      .map(x => x -> Hash.distance(Hash.decodeHash(center), Hash.decodeHash(x._2.toLong), unit))
                      .filter(_._2 <= radius)
                      .toList
                  val orderedList = order.fold(list) {
                    case Order.Ascending  => list.sortWith(_._2 < _._2)
                    case Order.Descending => list.sortWith(_._2 > _._2)
                  }
                  val countedList = count.fold(orderedList)(orderedList.take)
                  for {
                    _ <- store.fold(STM.unit)(key =>
                           putSortedSet(
                             key,
                             countedList.map { case ((name, score), _) =>
                               (name, score)
                             }.toMap
                           )
                         )
                    _ <- storeDist.fold(STM.unit)(key =>
                           putSortedSet(
                             key,
                             countedList.map { case ((name, _), distance) =>
                               (name, distance)
                             }.toMap
                           )
                         )
                    chunk = Chunk.fromIterable(
                              countedList.map { case ((name, score), distance) =>
                                val nameResp = RespValue.bulkString(name)
                                val infoChunk =
                                  withCoord.fold[Chunk[RespValue]](Chunk.empty) { _ =>
                                    val longLat = Hash.decodeHash(score.toLong)

                                    Chunk.single(
                                      RespValue.array(
                                        RespValue.bulkString(longLat.longitude.toString),
                                        RespValue.bulkString(longLat.latitude.toString)
                                      )
                                    )
                                  } ++
                                    withDist.fold[Chunk[RespValue]](Chunk.empty)(_ =>
                                      Chunk.single(RespValue.bulkString(distance.toString))
                                    ) ++
                                    withHash.fold[Chunk[RespValue]](Chunk.empty)(_ =>
                                      Chunk.single(RespValue.Integer(score.toLong))
                                    )

                                if (infoChunk.isEmpty) nameResp else RespValue.Array(nameResp +: infoChunk)
                              }
                            )
                  } yield
                    if (store.nonEmpty || storeDist.nonEmpty) RespValue.Integer(countedList.size.toLong)
                    else if (chunk.nonEmpty) RespValue.Array(chunk)
                    else RespValue.NullArray
                } else STM.succeedNow(RespValue.Error("ERR could not decode requested zset member"))
              }
            }
          )
        }

      case api.Keys.Exists | api.Keys.Touch =>
        STM
          .foldLeft(input)(0L) { case (acc, key) =>
            STM.ifM(keys.contains(key.asString))(STM.succeedNow(acc + 1), STM.succeedNow(acc))
          }
          .map(RespValue.Integer)

      case api.Keys.Del | api.Keys.Unlink =>
        STM
          .foldLeft(input)(0L) { case (acc, key) => delete(key.asString).map(acc + _) }
          .map(RespValue.Integer)

      case api.Keys.Keys =>
        val pattern = input.head.asString
        val matcher = FileSystems.getDefault.getPathMatcher("glob:" + pattern)
        keys.keys
          .map(keys => keys.filter(k => matcher.matches(Paths.get(k))))
          .map(Replies.array)

      case api.Keys.Scan =>
        val start   = input.head.asString.toInt
        val options = input.drop(1).map(_.asString)

        val countIndex = options.indexOf("COUNT")
        val count      = if (countIndex >= 0) Some(options(countIndex + 1).toInt) else None

        val patternIndex = options.indexOf("MATCH")
        val pattern      = if (patternIndex >= 0) Some(options(patternIndex + 1)) else None

        val redisTypeIndex = options.indexOf("TYPE")
        val redisType      = if (redisTypeIndex >= 0) Some(options(redisTypeIndex + 1)) else None

        val end = start + count.getOrElse(10)

        for {
          keys  <- keys.toList
          sliced = keys.slice(start, end)
          filtered = redisType.map { rt =>
                       sliced.filter { case (_, info) => info.redisType.stringify == rt }
                     }.getOrElse(sliced)
          matched = pattern.map { p =>
                      val matcher = FileSystems.getDefault.getPathMatcher("glob:" + p)
                      filtered.filter { case (k, _) => matcher.matches(Paths.get(k)) }
                    }.getOrElse(filtered)
          result    = matched.map(_._1)
          nextIndex = if (filtered.size <= end) 0 else end
        } yield RespValue.array(RespValue.bulkString(nextIndex.toString), Replies.array(result))

      case api.Keys.TypeOf =>
        val key = input.head.asString
        keys.get(key).map(info => info.map(_.redisType).fold("none")(_.stringify)).map(RespValue.SimpleString)

      case api.Keys.RandomKey =>
        for {
          ks   <- keys.keys
          pick <- selectOne(ks.toVector, randomPick)
        } yield pick.fold(RespValue.NullBulkString: RespValue)(RespValue.bulkString)

      case api.Keys.Rename =>
        val key    = input(0).asString
        val newkey = input(1).asString
        rename(key, newkey).as(Replies.Ok).catchAll(error => STM.succeedNow(error))

      case api.Keys.RenameNx =>
        val key    = input(0).asString
        val newkey = input(1).asString
        STM
          .ifM(keys.contains(newkey))(STM.succeedNow(0L), rename(key, newkey).as(1L))
          .map(RespValue.Integer)
          .catchAll(error => STM.succeedNow(error))

      case api.Keys.Sort =>
        val key     = input.head.asString
        val options = input.drop(1).map(_.asString)

        implicit val ordering: Ordering[String] =
          if (options.contains("DESC")) Ordering.String.reverse else Ordering.String
        val byIndex     = options.indexOf("BY")
        val by          = if (byIndex >= 0) Some(options(byIndex + 1)) else None
        val limitIndex  = options.indexOf("LIMIT")
        val limit       = if (limitIndex >= 0) Some((options(limitIndex + 1).toInt, options(limitIndex + 2).toInt)) else None
        val getIndexes  = options.zipWithIndex.withFilter(_._1 == "GET").map(_._2)
        val getPatterns = if (getIndexes.nonEmpty) getIndexes.map(i => options(i + 1)) else Chunk("#")
        val storeIndex  = options.indexOf("STORE")
        val storeKey    = if (storeIndex >= 0) Some(options(storeIndex + 1)) else None

        def sort(list: Chunk[String]) =
          by.fold(STM.succeedNow(list.sorted)) { by =>
            val pairs = list.foldLeft(STM.succeedNow(Chunk[(String, String)]())) { case (aggr, next) =>
              val key   = by.replace("*", next)
              val value = strings.get(key)
              aggr.flatMap(c => value.map(vo => vo.fold(c)(vo => c :+ (next -> vo))))
            }
            pairs.map(_.sortBy(_._2).map(_._1))
          }

        def get(list: Chunk[String]) =
          list.foldLeft(STM.succeedNow(Chunk[String]())) { case (aggr, next) =>
            val keys = getPatterns.map(p => p.replace("*", next))
            val values = keys.foldLeft(STM.succeedNow(Chunk[String]())) { case (all, k) =>
              if (k == "#") all.map(a => a :+ next)
              else all.flatMap(a => strings.get(k).map(v => v.fold(a)(a :+ _)))
            }
            aggr.flatMap(c => values.map(vs => c ++ vs))
          }

        def handle(listOpt: Option[Chunk[String]]) =
          for {
            list   <- listOpt.fold[STM[RespValue.Error, Chunk[String]]](STM.fail(Replies.Error))(STM.succeedNow)
            sorted <- sort(list)
            sliced  = limit.fold(sorted) { case (offset, count) => sorted.drop(offset).take(count) }
            result <- get(sliced)
          } yield result

        val zstm = for {
          keyInfoOpt <- keys.get(key)
          keyInfo    <- keyInfoOpt.fold[STM[RespValue.Error, KeyInfo]](STM.fail(Replies.Error))(STM.succeedNow)
          result <- keyInfo.`type` match {
                      case KeyType.Lists => lists.get(key).flatMap(handle)
                      case KeyType.Sets  => sets.get(key).flatMap(s => handle(s.map(Chunk.fromIterable)))
                      case KeyType.SortedSets =>
                        sortedSets.get(key).flatMap(s => handle(s.map(ks => Chunk.fromIterable(ks.keys))))
                      case _ => STM.fail(Replies.Error)
                    }
          _ <- storeKey.fold(ZSTM.unit)(sk => putList(sk, result))
        } yield if (storeKey.isEmpty) Replies.array(result) else RespValue.Integer(result.size.toLong)
        zstm.catchAll(error => STM.succeedNow(error))

      case api.Keys.Ttl =>
        val key = input.head.asString
        STM
          .ifM(keys.contains(key))(
            ttlOf(key, now).map(_.fold(-1L)(_.getSeconds)),
            STM.succeedNow(-2L)
          )
          .map(RespValue.Integer)

      case api.Keys.PTtl =>
        val key = input.head.asString
        STM
          .ifM(keys.contains(key))(
            ttlOf(key, now).map(_.fold(-1L)(_.toMillis)),
            STM.succeedNow(-2L)
          )
          .map(RespValue.Integer)

      case api.Keys.Persist =>
        val key = input.head.asString
        keys
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(info => keys.put(key, info.copy(expireAt = None)).as(1L)))
          .map(RespValue.Integer)

      case api.Keys.Expire =>
        val key      = input(0).asString
        val unixtime = now.plusSeconds(input(1).asLong)
        keys
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(info => keys.put(key, info.copy(expireAt = Some(unixtime))).as(1L)))
          .map(RespValue.Integer)

      case api.Keys.ExpireAt =>
        val key      = input(0).asString
        val unixtime = Instant.ofEpochSecond(input(1).asLong)
        keys
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(info => keys.put(key, info.copy(expireAt = Some(unixtime))).as(1L)))
          .map(RespValue.Integer)

      case api.Keys.PExpire =>
        val key      = input(0).asString
        val unixtime = now.plusMillis(input(1).asLong)
        keys
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(info => keys.put(key, info.copy(expireAt = Some(unixtime))).as(1L)))
          .map(RespValue.Integer)

      case api.Keys.PExpireAt =>
        val key      = input(0).asString
        val unixtime = Instant.ofEpochMilli(input(1).asLong)
        keys
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(info => keys.put(key, info.copy(expireAt = Some(unixtime))).as(1L)))
          .map(RespValue.Integer)

      case api.Sets.SAdd =>
        val key = input.head.asString
        orWrongType(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet ++ values
              added   = newSet.size - oldSet.size
              _      <- putSet(key, newSet)
            } yield RespValue.Integer(added.toLong)
          }
        )

      case api.Sets.SCard =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          sets.get(key).map(_.fold(RespValue.Integer(0))(s => RespValue.Integer(s.size.toLong)))
        )

      case api.Sets.SDiff =>
        val allkeys = input.map(_.asString)
        val mainKey = allkeys.head
        val others  = allkeys.tail

        orWrongType(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
          } yield RespValue.array(result.map(RespValue.bulkString).toList: _*)
        )

      case api.Sets.SDiffStore =>
        val allkeys = input.map(_.asString)
        val distkey = allkeys.head
        val mainKey = allkeys(1)
        val others  = allkeys.drop(2)

        orWrongType(forAll(allkeys)(isSet))(
          for {
            main   <- sets.getOrElse(mainKey, Set.empty)
            result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
            _      <- putSet(distkey, result)
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.Sets.SInter =>
        val keys      = input.map(_.asString)
        val mainKey   = keys.head
        val otherKeys = keys.tail
        sInter(mainKey, otherKeys).fold(_ => Replies.WrongType, Replies.array)

      case api.Sets.SInterStore =>
        val keys        = input.map(_.asString)
        val destination = keys.head
        val mainKey     = keys(1)
        val otherKeys   = keys.tail

        (STM.fail(()).unlessM(isSet(destination)) *> sInter(mainKey, otherKeys)).foldM(
          _ => STM.succeedNow(Replies.WrongType),
          s =>
            for {
              _ <- putSet(destination, s)
            } yield RespValue.Integer(s.size.toLong)
        )

      case api.Sets.SIsMember =>
        val key    = input.head.asString
        val member = input(1).asString

        orWrongType(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = if (set.contains(member)) RespValue.Integer(1) else RespValue.Integer(0)
          } yield result
        )

      case api.Sets.SMove =>
        val sourceKey      = input.head.asString
        val destinationKey = input(1).asString
        val member         = input(2).asString

        orWrongType(isSet(sourceKey))(
          sets.getOrElse(sourceKey, Set.empty).flatMap { source =>
            if (source.contains(member))
              STM.ifM(isSet(destinationKey))(
                for {
                  dest <- sets.getOrElse(destinationKey, Set.empty)
                  _    <- putSet(sourceKey, source - member)
                  _    <- putSet(destinationKey, dest + member)
                } yield RespValue.Integer(1),
                STM.succeedNow(Replies.WrongType)
              )
            else STM.succeedNow(RespValue.Integer(0))
          }
        )

      case api.Sets.SPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isSet(key))(
          for {
            set   <- sets.getOrElse(key, Set.empty)
            result = set.take(count)
            _     <- putSet(key, set -- result)
          } yield Replies.array(result)
        )

      case api.Sets.SMembers =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          sets.get(key).map(_.fold(Replies.EmptyArray)(Replies.array(_)))
        )

      case api.Sets.SRandMember =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          {
            val maybeCount = input.tail.headOption.map(b => b.asString.toLong)
            for {
              set     <- sets.getOrElse(key, Set.empty[String])
              asVector = set.toVector
              res <- maybeCount match {
                       case None =>
                         selectOne[String](asVector, randomPick).map {
                           _.fold(RespValue.NullBulkString: RespValue)(RespValue.bulkString)
                         }
                       case Some(n) if n > 0 => selectN(asVector, n, randomPick).map(Replies.array)
                       case Some(n) if n < 0 => selectNWithReplacement(asVector, -n, randomPick).map(Replies.array)
                       case Some(_)          => STM.succeedNow(RespValue.NullBulkString)
                     }
            } yield res
          }
        )

      case api.Sets.SRem =>
        val key = input.head.asString

        orWrongType(isSet(key))(
          {
            val values = input.tail.map(_.asString)
            for {
              oldSet <- sets.getOrElse(key, Set.empty)
              newSet  = oldSet -- values
              removed = oldSet.size - newSet.size
              _      <- putSet(key, newSet)
            } yield RespValue.Integer(removed.toLong)
          }
        )

      case api.Sets.SUnion =>
        val keys = input.map(_.asString)

        orWrongType(forAll(keys)(isSet))(
          STM
            .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
              sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                unionSoFar ++ currentSet
              }
            }
            .map(unionSet => Replies.array(unionSet))
        )

      case api.Sets.SUnionStore =>
        val destination = input.head.asString
        val keys        = input.tail.map(_.asString)

        orWrongType(forAll(keys)(isSet))(
          for {
            union <- STM
                       .foldLeft(keys)(Set.empty[String]) { (unionSoFar, nextKey) =>
                         sets.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                           unionSoFar ++ currentSet
                         }
                       }
            _ <- putSet(destination, union)
          } yield RespValue.Integer(union.size.toLong)
        )

      case api.Sets.SScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2) input(2).asString match {
            case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
            case _       => None
          }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isSet(key))(
          {
            for {
              set      <- sets.getOrElse(key, Set.empty)
              filtered  = maybeRegex.map(regex => set.filter(s => regex.pattern.matcher(s).matches)).getOrElse(set)
              resultSet = filtered.slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              results   = Replies.array(resultSet)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
        )

      case api.Strings.Get =>
        val key = input.head.asString
        orWrongType(isString(key))(
          strings.get(key).map(_.fold(RespValue.NullBulkString: RespValue)(RespValue.bulkString))
        )

      case api.Strings.Set =>
        // not a full implementation. Just enough to make set tests work
        val key   = input(0).asString
        val value = input(1).asString
        orWrongType(isString(key))(
          putString(key, value).as(Replies.Ok)
        )

      case api.Strings.PSetEx =>
        val key      = input(0).asString
        val unixtime = Instant.ofEpochMilli(input(1).asLong)
        val value    = input(2).asString
        orWrongType(isString(key))(
          putString(key, value, Some(unixtime)).as(Replies.Ok)
        )

      case api.HyperLogLog.PfAdd =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString).toSet

        orWrongType(isHyperLogLog(key))(
          for {
            oldValues <- hyperLogLogs.getOrElse(key, Set.empty)
            ret        = if (oldValues == values) 0L else 1L
            _         <- putHyperLogLog(key, values)
          } yield RespValue.Integer(ret)
        )

      case api.HyperLogLog.PfCount =>
        val keys = input.map(_.asString)
        orWrongType(forAll(keys)(isHyperLogLog))(
          STM
            .foldLeft(keys)(Set.empty[String]) { (bHyperLogLogs, nextKey) =>
              hyperLogLogs.getOrElse(nextKey, Set.empty[String]).map { currentSet =>
                bHyperLogLogs ++ currentSet
              }
            }
            .map(vs => RespValue.Integer(vs.size.toLong))
        )

      case api.HyperLogLog.PfMerge =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(forAll(values ++ Chunk.single(key))(isHyperLogLog))(
          for {
            sourceValues <- STM.foldLeft(values)(Set.empty: Set[String]) { (bHyperLogLogs, nextKey) =>
                              hyperLogLogs.getOrElse(nextKey, Set.empty).map { currentSet =>
                                bHyperLogLogs ++ currentSet
                              }
                            }
            destValues <- hyperLogLogs.getOrElse(key, Set.empty)
            putValues   = sourceValues ++ destValues
            _          <- hyperLogLogs.put(key, putValues)
          } yield Replies.Ok
        )

      case api.Lists.LIndex =>
        val key   = input.head.asString
        val index = input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = if (index < 0) list.lift(list.size + index) else list.lift(index)
          } yield result.fold[RespValue](RespValue.NullBulkString)(value => RespValue.bulkString(value))
        )

      case api.Lists.LInsert =>
        val key = input.head.asString
        val position = input(1).asString match {
          case "BEFORE" => Position.Before
          case "AFTER"  => Position.After
        }
        val pivot   = input(2).asString
        val element = input(3).asString

        orWrongType(isList(key))(
          for {
            maybeList <- lists.get(key)
            eitherResult = maybeList match {
                             case None => Left(RespValue.Integer(0L))
                             case Some(list) =>
                               Right(
                                 position match {
                                   case Position.Before =>
                                     list.find(_ == pivot).map { p =>
                                       val index        = list.indexOf(p)
                                       val (head, tail) = list.splitAt(index)
                                       head ++ Chunk.single(element) ++ tail
                                     }

                                   case Position.After =>
                                     list.find(_ == pivot).map { p =>
                                       val index        = list.indexOf(p)
                                       val (head, tail) = list.splitAt(index + 1)
                                       head ++ Chunk.single(element) ++ tail
                                     }
                                 }
                               )
                           }
            result <- eitherResult.fold(
                        respValue => STM.succeedNow(respValue),
                        maybeList =>
                          maybeList.fold(STM.succeedNow(RespValue.Integer(-1L)))(insert =>
                            putList(key, insert) *> STM.succeedNow(RespValue.Integer(insert.size.toLong))
                          )
                      )
          } yield result
        )

      case api.Lists.LLen =>
        val key = input.head.asString

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
          } yield RespValue.Integer(list.size.toLong)
        )

      case api.Lists.LPush =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          for {
            oldValues <- lists.getOrElse(key, Chunk.empty)
            newValues  = values.reverse ++ oldValues
            _         <- putList(key, newValues)
          } yield RespValue.Integer(newValues.size.toLong)
        )

      case api.Lists.LPushX =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          (for {
            list    <- lists.get(key)
            newList <- STM.fromOption(list.map(oldValues => values.reverse ++ oldValues))
            _       <- putList(key, newList)
          } yield newList).fold(_ => RespValue.Integer(0L), result => RespValue.Integer(result.size.toLong))
        )

      case api.Lists.LRange =>
        val key   = input.head.asString
        val start = input(1).asString.toInt
        val end   = input(2).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = if (end < 0) list.slice(start, list.size + 1 + end) else list.slice(start, end + 1)
          } yield Replies.array(result)
        )

      case api.Lists.LRem =>
        val key     = input.head.asString
        val count   = input(1).asString.toInt
        val element = input(2).asString

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
            result = count match {
                       case 0          => list.filterNot(_ == element)
                       case n if n > 0 => dropWhileLimit(list)(_ == element, n)
                       case n if n < 0 => dropWhileLimit(list.reverse)(_ == element, n * (-1)).reverse
                     }
            _ <- putList(key, result)
          } yield RespValue.Integer((list.size - result.size).toLong)
        )

      case api.Lists.LSet =>
        val key     = input.head.asString
        val index   = input(1).asString.toInt
        val element = input(2).asString

        orWrongType(isList(key))(
          (for {
            list <- lists.getOrElse(key, Chunk.empty)
            newList <- STM.fromOption {
                         Try {
                           if (index < 0) list.updated(list.size - 1 + index, element)
                           else list.updated(index, element)
                         }.toOption
                       }
            _ <- putList(key, newList)
          } yield ()).fold(_ => RespValue.Error("ERR index out of range"), _ => RespValue.SimpleString("OK"))
        )

      case api.Lists.LTrim =>
        val key   = input.head.asString
        val start = input(1).asString.toInt
        val stop  = input(2).asString.toInt

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty)
            result = (start, stop) match {
                       case (l, r) if l >= 0 && r >= 0 => list.slice(l, r + 1)
                       case (l, r) if l < 0 && r >= 0  => list.slice(list.size + l, r + 1)
                       case (l, r) if l >= 0 && r < 0  => list.slice(l, list.size + r + 1)
                       case (l, r) if l < 0 && r < 0   => list.slice(list.size + l, list.size + r + 1)
                       case (_, _)                     => list
                     }
            _ <- putList(key, result)
          } yield RespValue.SimpleString("OK")
        )

      case api.Lists.RPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = list.takeRight(count).reverse
            _     <- putList(key, list.dropRight(count))
          } yield result.size match {
            case 0 => RespValue.NullBulkString
            case 1 => RespValue.bulkString(result.head)
            case _ => Replies.array(result)
          }
        )

      case api.Lists.RPopLPush =>
        val source      = input(0).asString
        val destination = input(1).asString

        orWrongType(forAll(Chunk(source, destination))(isList))(
          for {
            sourceList       <- lists.getOrElse(source, Chunk.empty)
            destinationList  <- lists.getOrElse(destination, Chunk.empty)
            value             = sourceList.lastOption
            sourceResult      = sourceList.dropRight(1)
            destinationResult = value.map(_ +: destinationList).getOrElse(destinationList)
            _                <- putList(source, sourceResult)
            _                <- putList(destination, destinationResult)
          } yield value.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result))
        )

      case api.Lists.RPush =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          for {
            oldValues <- lists.getOrElse(key, Chunk.empty)
            newValues  = values ++ oldValues
            _         <- putList(key, newValues)
          } yield RespValue.Integer(newValues.size.toLong)
        )

      case api.Lists.LPop =>
        val key   = input.head.asString
        val count = if (input.size == 1) 1 else input(1).asString.toInt

        orWrongType(isList(key))(
          for {
            list  <- lists.getOrElse(key, Chunk.empty)
            result = list.take(count)
            _     <- putList(key, list.drop(count))
          } yield result.size match {
            case 0 => RespValue.NullBulkString
            case 1 => RespValue.bulkString(result.head)
            case _ => Replies.array(result)
          }
        )

      case api.Lists.RPushX =>
        val key    = input.head.asString
        val values = input.tail.map(_.asString)

        orWrongType(isList(key))(
          (for {
            list    <- lists.get(key)
            newList <- STM.fromOption(list.map(oldValues => oldValues ++ values.toVector))
            _       <- putList(key, newList)
          } yield newList).fold(_ => RespValue.Integer(0L), result => RespValue.Integer(result.size.toLong))
        )

      case api.Lists.BlPop =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isList))(
          (for {
            allLists <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& lists.getOrElse(key, Chunk.empty)))(identity)
            nonEmptyLists <- STM.succeed(allLists.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)      <- STM.fromOption(nonEmptyLists.headOption)
            _             <- putList(sk, sl.tail)
          } yield Replies.array(Chunk(sk, sl.head))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.BrPop =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isList))(
          (for {
            allLists <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& lists.getOrElse(key, Chunk.empty)))(identity)
            nonEmptyLists <- STM.succeed(allLists.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)      <- STM.fromOption(nonEmptyLists.headOption)
            _             <- putList(sk, sl.dropRight(1))
          } yield Replies.array(Chunk(sk, sl.last))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.BrPopLPush =>
        val source      = input.head.asString
        val destination = input.tail.head.asString

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceListOpt    <- lists.get(source)
            sourceList       <- STM.fromOption(sourceListOpt)
            destinationList  <- lists.getOrElse(destination, Chunk.empty)
            value            <- STM.fromOption(sourceList.lastOption)
            sourceResult      = sourceList.dropRight(1)
            destinationResult = value +: destinationList
            _                <- putList(source, sourceResult)
            _                <- putList(destination, destinationResult)
          } yield RespValue.bulkString(value)).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.Lists.LMove =>
        val source      = input(0).asString
        val destination = input(1).asString
        val sourceSide = input(2).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }
        val destinationSide = input(3).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceList      <- lists.get(source) >>= (l => STM.fromOption(l))
            destinationList <- lists.getOrElse(destination, Chunk.empty)
            element <- STM.fromOption(sourceSide match {
                         case Side.Left  => sourceList.headOption
                         case Side.Right => sourceList.lastOption
                       })
            newSourceList = sourceSide match {
                              case Side.Left  => sourceList.drop(1)
                              case Side.Right => sourceList.dropRight(1)
                            }
            newDestinationList =
              if (source != destination)
                destinationSide match {
                  case Side.Left  => element +: destinationList
                  case Side.Right => destinationList :+ element
                }
              else
                destinationSide match {
                  case Side.Left  => element +: newSourceList
                  case Side.Right => newSourceList :+ element
                }
            _ <- if (source != destination)
                   putList(source, newSourceList) *> putList(destination, newDestinationList)
                 else
                   putList(source, newDestinationList)
          } yield element).fold(_ => RespValue.NullBulkString, result => RespValue.bulkString(result))
        )

      case api.Lists.BlMove =>
        val source      = input(0).asString
        val destination = input(1).asString
        val sourceSide = input(2).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }
        val destinationSide = input(3).asString match {
          case "LEFT"  => Side.Left
          case "RIGHT" => Side.Right
        }

        orWrongType(forAll(Chunk(source, destination))(isList))(
          (for {
            sourceList      <- lists.get(source) >>= (l => STM.fromOption(l))
            destinationList <- lists.getOrElse(destination, Chunk.empty)
            element <- STM.fromOption(sourceSide match {
                         case Side.Left  => sourceList.headOption
                         case Side.Right => sourceList.lastOption
                       })
            newSourceList = sourceSide match {
                              case Side.Left  => sourceList.drop(1)
                              case Side.Right => sourceList.dropRight(1)
                            }
            newDestinationList =
              if (source != destination)
                destinationSide match {
                  case Side.Left  => element +: destinationList
                  case Side.Right => destinationList :+ element
                }
              else
                destinationSide match {
                  case Side.Left  => element +: newSourceList
                  case Side.Right => newSourceList :+ element
                }
            _ <- if (source != destination)
                   putList(source, newSourceList) *> putList(destination, newDestinationList)
                 else
                   putList(source, newDestinationList)
          } yield element).foldM(_ => STM.retry, result => STM.succeed(RespValue.bulkString(result)))
        )

      case api.Lists.LPos =>
        val key     = input(0).asString
        val element = input(1).asString

        val options      = input.map(_.asString).zipWithIndex
        val rankOption   = options.find(_._1 == "RANK").map(_._2).map(idx => input(idx + 1).asLong)
        val countOption  = options.find(_._1 == "COUNT").map(_._2).map(idx => input(idx + 1).asLong)
        val maxLenOption = options.find(_._1 == "MAXLEN").map(_._2).map(idx => input(idx + 1).asLong)

        orWrongType(isList(key))(
          for {
            list <- lists.getOrElse(key, Chunk.empty).map { list =>
                      (maxLenOption, rankOption) match {
                        case (Some(maxLen), None)                   => list.take(maxLen.toInt)
                        case (Some(maxLen), Some(rank)) if rank < 0 => list.reverse.take(maxLen.toInt).reverse
                        case (_, _)                                 => list
                      }
                    }
            idxList = list.zipWithIndex
            result = (countOption, rankOption) match {
                       case (Some(count), Some(rank)) if rank < 0 =>
                         Right(
                           idxList.reverse
                             .filter(_._1 == element)
                             .drop((rank.toInt * -1) - 1)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (Some(count), Some(rank)) =>
                         Right(
                           idxList
                             .filter(_._1 == element)
                             .drop(rank.toInt - 1)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (Some(count), None) =>
                         Right(
                           idxList
                             .filter(_._1 == element)
                             .take(count.toInt)
                             .map(_._2)
                         )
                       case (None, Some(rank)) if rank < 0 =>
                         Left(
                           idxList.reverse
                             .filter(_._1 == element)
                             .drop(rank.toInt)
                             .map(_._2)
                             .headOption
                         )
                       case (None, Some(rank)) =>
                         Left(
                           idxList
                             .filter(_._1 == element)
                             .drop(rank.toInt - 1)
                             .map(_._2)
                             .headOption
                         )
                       case (None, None) =>
                         Left(
                           idxList
                             .filter(_._1 == element)
                             .map(_._2)
                             .headOption
                         )
                     }

          } yield result.fold(
            left => left.fold[RespValue](RespValue.NullBulkString)(value => RespValue.Integer(value.toLong)),
            right => RespValue.array(right.map(value => RespValue.Integer(value.toLong)): _*)
          )
        )

      case api.Hashes.HDel =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash       <- hashes.getOrElse(key, Map.empty)
            countExists = hash.keys count values.contains
            newHash     = hash -- values
            _          <- ZSTM.ifM(ZSTM.succeedNow(newHash.isEmpty))(delete(key), putHash(key, newHash))
          } yield RespValue.Integer(countExists.toLong)
        )

      case api.Hashes.HExists =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            exists = hash.keys.exists(_ == field)
          } yield if (exists) RespValue.Integer(1L) else RespValue.Integer(0L)
        )

      case api.Hashes.HGet =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
            value = hash.get(field)
          } yield value.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result))
        )

      case api.Hashes.HGetAll =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash   <- hashes.getOrElse(key, Map.empty)
            results = hash.flatMap { case (k, v) => Iterable.apply(k, v) } map RespValue.bulkString
          } yield RespValue.Array(Chunk.fromIterable(results))
        )

      case api.Hashes.HIncrBy =>
        val key   = input(0).asString
        val field = input(1).asString
        val incr  = input(2).asString.toLong

        orWrongType(isHash(key))(
          (for {
            hash     <- hashes.getOrElse(key, Map.empty)
            newValue <- STM.fromTry(Try(hash.getOrElse(field, "0").toLong + incr))
            newMap    = hash + (field -> newValue.toString)
            _        <- putHash(key, newMap)
          } yield newValue).fold(_ => Replies.Error, result => RespValue.Integer(result))
        )

      case api.Hashes.HIncrByFloat =>
        val key   = input(0).asString
        val field = input(1).asString
        val incr  = input(2).asString.toDouble

        orWrongType(isHash(key))(
          (for {
            hash     <- hashes.getOrElse(key, Map.empty)
            newValue <- STM.fromTry(Try(hash.getOrElse(field, "0").toDouble + incr))
            newHash   = hash + (field -> newValue.toString)
            _        <- putHash(key, newHash)
          } yield newValue).fold(_ => Replies.Error, result => RespValue.bulkString(result.toString))
        )

      case api.Hashes.HKeys =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield RespValue.Array(Chunk.fromIterable(hash.keys map RespValue.bulkString))
        )

      case api.Hashes.HLen =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield RespValue.Integer(hash.size.toLong)
        )

      case api.Hashes.HmGet =>
        val key    = input(0).asString
        val fields = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            result = fields.map(hash.get)
          } yield RespValue.Array(result.map {
            case None        => RespValue.NullBulkString
            case Some(value) => RespValue.bulkString(value)
          })
        )

      case api.Hashes.HmSet =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            newMap = hash ++ values.grouped(2).map(g => (g(0), g(1)))
            _     <- putHash(key, newMap)
          } yield Replies.Ok
        )

      case api.Hashes.HScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2)
            input(2).asString match {
              case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
              case _       => None
            }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isHash(key))(
          for {
            set <- hashes.getOrElse(key, Map.empty)
            filtered =
              maybeRegex.map(regex => set.filter { case (k, _) => regex.pattern.matcher(k).matches }).getOrElse(set)
            resultSet = filtered.slice(start, end)
            nextIndex = if (filtered.size <= end) 0 else end
            results   = Replies.array(resultSet.flatMap { case (k, v) => Iterable(k, v) })
          } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
        )

      case api.Hashes.HSet =>
        val key    = input(0).asString
        val values = input.tail.map(_.asString)

        orWrongType(isHash(key))(
          for {
            hash   <- hashes.getOrElse(key, Map.empty)
            newHash = hash ++ values.grouped(2).map(g => (g(0), g(1)))
            _      <- putHash(key, newHash)
          } yield RespValue.Integer(newHash.size.toLong - hash.size.toLong)
        )

      case api.Hashes.HSetNx =>
        val key   = input(0).asString
        val field = input(1).asString
        val value = input(2).asString

        orWrongType(isHash(key))(
          for {
            hash    <- hashes.getOrElse(key, Map.empty)
            contains = hash.contains(field)
            newHash  = hash ++ (if (contains) Map.empty else Map(field -> value))
            _       <- putHash(key, newHash)
          } yield RespValue.Integer(if (contains) 0L else 1L)
        )

      case api.Hashes.HStrLen =>
        val key   = input(0).asString
        val field = input(1).asString

        orWrongType(isHash(key))(
          for {
            hash <- hashes.getOrElse(key, Map.empty)
            len   = hash.get(field).map(_.length.toLong).getOrElse(0L)
          } yield RespValue.Integer(len)
        )

      case api.Hashes.HVals =>
        val key = input(0).asString

        orWrongType(isHash(key))(
          for {
            hash  <- hashes.getOrElse(key, Map.empty)
            values = hash.values map RespValue.bulkString
          } yield RespValue.Array(Chunk.fromIterable(values))
        )

      case api.Hashes.HRandField =>
        val key        = input(0).asString
        val count      = if (input.size == 1) None else Some(input(1).asString.toLong)
        val withValues = if (input.size == 3) input(2).asString == "WITHVALUES" else false

        def selectValues[T](n: Long, values: Vector[T]) = {
          val repeatedAllowed = n < 0
          val count           = Math.abs(n)
          val t               = count - values.length

          if (repeatedAllowed && t > 0) {
            selectNWithReplacement[T](values, count, randomPick)
          } else {
            selectN[T](values, count, randomPick)
          }
        }

        orWrongType(isHash(key)) {
          val keysAndValues = for {
            hash <- hashes.getOrElse(key, Map.empty)
          } yield (hash.keys map RespValue.bulkString) zip (hash.values map RespValue.bulkString)

          if (count.isDefined && withValues) {
            for {
              kvs            <- keysAndValues
              fields         <- selectValues(count.get, kvs.toVector)
              fieldsAndValues = fields.flatMap { case (k, v) => Seq(k, v) }
            } yield RespValue.Array(Chunk.fromIterable(fieldsAndValues))
          } else if (count.isDefined) {
            for {
              kvs    <- keysAndValues
              keys    = kvs.map(_._1)
              fields <- selectValues(count.get, keys.toVector)
            } yield RespValue.Array(Chunk.fromIterable(fields))
          } else {
            for {
              kvs <- keysAndValues
              keys = kvs.map { case (k, _) => k }
              key <- selectOne[zio.redis.RespValue.BulkString](keys.toVector, randomPick)
            } yield key.getOrElse(RespValue.NullBulkString)
          }
        }

      case api.SortedSets.BzPopMax =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isSortedSet))(
          (for {
            allSets <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& sortedSets.getOrElse(key, Map.empty)))(identity)
            nonEmpty    <- STM.succeed(allSets.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)    <- STM.fromOption(nonEmpty.headOption)
            (maxM, maxV) = sl.toList.maxBy(_._2)
            _           <- putSortedSet(sk, sl - maxM)
          } yield Replies.array(Chunk(sk, maxM, maxV.toString))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.SortedSets.BzPopMin =>
        val keys = input.dropRight(1).map(_.asString)

        orWrongType(forAll(keys)(isSortedSet))(
          (for {
            allSets <-
              STM.foreach(keys.map(key => STM.succeedNow(key) &&& sortedSets.getOrElse(key, Map.empty)))(identity)
            nonEmpty    <- STM.succeed(allSets.collect { case (key, v) if v.nonEmpty => key -> v })
            (sk, sl)    <- STM.fromOption(nonEmpty.headOption)
            (maxM, maxV) = sl.toList.minBy(_._2)
            _           <- putSortedSet(sk, sl - maxM)
          } yield Replies.array(Chunk(sk, maxM, maxV.toString))).foldM(_ => STM.retry, result => STM.succeed(result))
        )

      case api.SortedSets.ZAdd =>
        val key = input(0).asString

        val updateOption = input.map(_.asString).find {
          case "XX" => true
          case "NX" => true
          case "LT" => true
          case "GT" => true
          case _    => false
        }

        val changedOption = input.map(_.asString).find {
          case "CH" => true
          case _    => false
        }

        val incrOption = input.map(_.asString).find {
          case "INCR" => true
          case _      => false
        }

        val optionsCount = updateOption.map(_ => 1).getOrElse(0) + changedOption.map(_ => 1).getOrElse(0) + incrOption
          .map(_ => 1)
          .getOrElse(0)

        val values =
          Chunk.fromIterator(
            input
              .drop(1 + optionsCount)
              .map(_.asString)
              .grouped(2)
              .map(g => MemberScore(g(0).toDouble, g(1)))
          )

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            valuesToAdd = updateOption.map {
                            case "XX" =>
                              values.filter(ms => scoreMap.contains(ms.member))

                            case "NX" =>
                              values.filter(ms => !scoreMap.contains(ms.member))

                            case "LT" =>
                              values.filter(ms =>
                                scoreMap.exists { case (member, score) =>
                                  (member == ms.member && score > ms.score) || (ms.member != member)
                                }
                              )

                            case "GT" =>
                              values.filter(ms =>
                                scoreMap.exists { case (member, score) =>
                                  (member == ms.member && score < ms.score) || (ms.member != member)
                                }
                              )
                          }.getOrElse(values)

            newScoreMap =
              if (incrOption.isDefined) {
                val ms = values.head
                scoreMap + (ms.member -> (scoreMap.getOrElse(ms.member, 0d) + ms.score))
              } else
                scoreMap ++ valuesToAdd.map(ms => ms.member -> ms.score)

            incrScore = incrOption.map { _ =>
                          val ms = values.head
                          scoreMap.getOrElse(ms.member, 0d) + ms.score
                        }

            valuesChanged = changedOption.map(_ => valuesToAdd.size).getOrElse(newScoreMap.size - scoreMap.size)
            _            <- putSortedSet(key, newScoreMap)
          } yield incrScore.fold[RespValue](RespValue.Integer(valuesChanged.toLong))(result =>
            RespValue.bulkString(result.toString)
          )
        )

      case api.SortedSets.ZCard =>
        val key = input(0).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
          } yield RespValue.Integer(scoreMap.size.toLong)
        )

      case api.SortedSets.ZCount =>
        val key = input(0).asString
        val min = input(1).asLong
        val max = input(2).asLong

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            result    = scoreMap.filter { case (_, score) => score >= min && score <= max }
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.SortedSets.ZDiff =>
        val numkeys          = input(0).asLong
        val keys             = input.drop(1).take(numkeys.toInt).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(forAll(keys)(isSortedSet))(
          for {
            sourceMaps <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))
            diffMap     = sourceMaps.reduce[Map[String, Double]] { case (a, b) => (a -- b.keySet) ++ (b -- a.keySet) }
            result =
              if (withScoresOption.isDefined)
                Chunk.fromIterable(diffMap.toArray.flatMap { case (v, s) =>
                  Chunk(bulkString(v), bulkString(s.toString))
                })
              else
                Chunk.fromIterable(diffMap.keys.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZDiffStore =>
        val destination = input(0).asString
        val numkeys     = input(1).asLong
        val keys        = input.drop(2).take(numkeys.toInt).map(_.asString)

        orWrongType(forAll(keys :+ destination)(isSortedSet))(
          for {
            sourceMaps <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))
            diffMap     = sourceMaps.reduce[Map[String, Double]] { case (a, b) => (a -- b.keySet) ++ (b -- a.keySet) }
            _          <- putSortedSet(destination, diffMap)
          } yield RespValue.Integer(diffMap.size.toLong)
        )

      case api.SortedSets.ZIncrBy =>
        val key       = input(0).asString
        val increment = input(1).asLong
        val member    = input(2).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            resultScore = scoreMap
                            .get(member)
                            .map(score => score + increment)
                            .getOrElse(increment.toDouble)

            resultSet = scoreMap + (member -> resultScore)
            _        <- putSortedSet(key, resultSet)
          } yield RespValue.bulkString(resultScore.toString)
        )

      case api.SortedSets.ZInter =>
        val numKeys          = input(0).asLong
        val keys             = input.drop(1).take(numKeys.toInt).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              intersectionKeys =
                sourceSets.map(_.keySet).reduce(_.intersect(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => intersectionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              intersectionMap =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              result =
                if (withScoresOption.isDefined)
                  Chunk.fromIterable(intersectionMap.toArray.sortBy(_._2).flatMap { case (v, s) =>
                    Chunk(bulkString(v), bulkString(s.toString))
                  })
                else
                  Chunk.fromIterable(intersectionMap.toArray.sortBy(_._2).map(e => bulkString(e._1)))

            } yield RespValue.Array(result)
          )
        )

      case api.SortedSets.ZInterStore =>
        val destination = input(0).asString
        val numKeys     = input(1).asLong.toInt
        val keys        = input.drop(2).take(numKeys).map(_.asString)

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys :+ destination)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              intersectionKeys =
                sourceSets.map(_.keySet).reduce(_.intersect(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => intersectionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              destinationResult =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              _ <- putSortedSet(destination, destinationResult)
            } yield RespValue.Integer(destinationResult.size.toLong)
          )
        )

      case api.SortedSets.ZLexCount =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            lexKeys   = scoreMap.keys.toArray.sorted

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            result = Chunk.fromIterable(filtered.map(bulkString))
          } yield RespValue.Integer(result.size.toLong)
        )

      case api.SortedSets.ZRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted)

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            bounds = (min, max) match {
                       case (LexMinimum.Unbounded, LexMaximum.Unbounded) => filtered
                       case (LexMinimum.Unbounded, _)                    => filtered.dropRight(1)
                       case (_, LexMaximum.Unbounded)                    => filtered.drop(1)
                       case (_, _)                                       => filtered.drop(1).dropRight(1)
                     }

            result = Chunk.fromIterable(bounds.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRevRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._1)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted).reverse

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s < key
                               case LexMinimum.Closed(key) => s <= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s > key
                               case LexMaximum.Closed(key) => s >= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            result = Chunk.fromIterable(filtered.map(bulkString))
          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRemRangeByLex =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-"                    => LexMinimum.Unbounded
          case s if s.startsWith("(") => LexMinimum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMinimum.Closed(s.drop(1))
        }

        val max = input(2).asString match {
          case "+"                    => LexMaximum.Unbounded
          case s if s.startsWith("(") => LexMaximum.Open(s.drop(1))
          case s if s.startsWith("[") => LexMaximum.Closed(s.drop(1))
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                            .map(_._1)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.keys.toArray.sorted)

            minPredicate = (s: String) =>
                             min match {
                               case LexMinimum.Unbounded   => true
                               case LexMinimum.Open(key)   => s > key
                               case LexMinimum.Closed(key) => s >= key
                             }

            maxPredicate = (s: String) =>
                             max match {
                               case LexMaximum.Unbounded   => true
                               case LexMaximum.Open(key)   => s < key
                               case LexMaximum.Closed(key) => s <= key
                             }

            filtered = lexKeys.filter(s => minPredicate(s) && maxPredicate(s))

            _ <- putSortedSet(key, scoreMap -- filtered)
          } yield RespValue.Integer(filtered.length.toLong)
        )

      case api.SortedSets.ZRemRangeByRank =>
        val key   = input(0).asString
        val start = input(1).asLong.toInt
        val stop  = input(2).asLong.toInt

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank      = scoreMap.toArray.sortBy(_._2)
            result    = rank.slice(start, if (stop < 0) rank.length + stop else stop + 1)
            _        <- putSortedSet(key, scoreMap -- result.map(_._1))
          } yield RespValue.Integer(result.length.toLong)
        )

      case api.SortedSets.ZRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s                      => ScoreMinimum.Closed(s.toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s                      => ScoreMaximum.Closed(s.toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        val withScoresOption = input.map(_.asString).indexOf("WITHSCORES") match {
          case -1 => false
          case _  => true
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2))

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s > key
                               case ScoreMinimum.Closed(key)      => s >= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s < key
                               case ScoreMaximum.Closed(key)      => s <= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            result =
              if (withScoresOption)
                Chunk.fromIterable(filtered.flatMap { case (k, s) => bulkString(k) :: bulkString(s.toString) :: Nil })
              else
                Chunk.fromIterable(filtered.map { case (k, _) => bulkString(k) })

          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRevRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s                      => ScoreMinimum.Closed(s.toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s                      => ScoreMaximum.Closed(s.toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        val withScoresOption = input.map(_.asString).indexOf("WITHSCORES") match {
          case -1 => false
          case _  => true
        }

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption.map(_ + 1)
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .reverse
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2).reverse)

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s < key
                               case ScoreMinimum.Closed(key)      => s <= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s > key
                               case ScoreMaximum.Closed(key)      => s >= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            result =
              if (withScoresOption)
                Chunk.fromIterable(filtered.flatMap { case (k, s) => bulkString(k) :: bulkString(s.toString) :: Nil })
              else
                Chunk.fromIterable(filtered.map { case (k, _) => bulkString(k) })

          } yield RespValue.Array(result)
        )

      case api.SortedSets.ZRemRangeByScore =>
        val key = input(0).asString

        val min = input(1).asString match {
          case "-inf"                 => ScoreMinimum.Infinity
          case s if s.startsWith("(") => ScoreMinimum.Open(s.drop(1).toDouble)
          case s if s.startsWith("[") => ScoreMinimum.Closed(s.drop(1).toDouble)
        }

        val max = input(2).asString match {
          case "+inf"                 => ScoreMaximum.Infinity
          case s if s.startsWith("(") => ScoreMaximum.Open(s.drop(1).toDouble)
          case s if s.startsWith("[") => ScoreMaximum.Closed(s.drop(1).toDouble)
        }

        val limitOptionIdx = input.map(_.asString).indexOf("LIMIT") match {
          case -1  => None
          case idx => Some(idx)
        }

        val offsetOption = limitOptionIdx.map(idx => input(idx + 1).asLong)
        val countOption  = limitOptionIdx.map(idx => input(idx + 2).asLong)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)

            limitKeys = for {
                          offset <- offsetOption
                          count  <- countOption
                        } yield {
                          scoreMap.toArray
                            .sortBy(_._2)
                            .slice(offset.toInt, offset.toInt + count.toInt)
                        }

            lexKeys = limitKeys.getOrElse(scoreMap.toArray.sortBy(_._2))

            minPredicate = (s: Double) =>
                             min match {
                               case _: ScoreMinimum.Infinity.type => true
                               case ScoreMinimum.Open(key)        => s > key
                               case ScoreMinimum.Closed(key)      => s >= key
                             }

            maxPredicate = (s: Double) =>
                             max match {
                               case _: ScoreMaximum.Infinity.type => true
                               case ScoreMaximum.Open(key)        => s < key
                               case ScoreMaximum.Closed(key)      => s <= key
                             }

            filtered = lexKeys.filter { case (_, s) => minPredicate(s) && maxPredicate(s) }

            _ <- putSortedSet(key, scoreMap -- filtered.map(_._1))
          } yield RespValue.Integer(filtered.length.toLong)
        )

      case api.SortedSets.ZPopMin =>
        val key   = input(0).asString
        val count = input.drop(1).headOption.map(_.asString.toInt).getOrElse(1)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            results   = scoreMap.toArray.sortBy { case (_, score) => score }.take(count)
            _        <- putSortedSet(key, scoreMap -- results.map(_._1))
          } yield RespValue.Array(
            Chunk
              .fromIterable(results)
              .flatMap(ms => Chunk(RespValue.bulkString(ms._1), RespValue.bulkString(ms._2.toString.stripSuffix(".0"))))
          )
        )

      case api.SortedSets.ZPopMax =>
        val key   = input(0).asString
        val count = input.drop(1).headOption.map(_.asString.toInt).getOrElse(1)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            results   = scoreMap.toArray.sortBy { case (_, score) => score }.reverse.take(count)
            _        <- putSortedSet(key, scoreMap -- results.map(_._1))
          } yield RespValue.Array(
            Chunk
              .fromIterable(results)
              .flatMap(ms => Chunk(RespValue.bulkString(ms._1), RespValue.bulkString(ms._2.toString.stripSuffix(".0"))))
          )
        )

      case api.SortedSets.ZRank =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank = scoreMap.toArray.sortBy(_._2).map(_._1).indexOf(member) match {
                     case -1  => None
                     case idx => Some(idx)
                   }
          } yield rank.fold[RespValue](RespValue.NullBulkString)(result => RespValue.Integer(result.toLong))
        )

      case api.SortedSets.ZRem =>
        val key     = input(0).asString
        val members = input.tail.map(_.asString)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            newSet    = scoreMap.filterNot { case (v, _) => members.contains(v) }
            _        <- putSortedSet(key, newSet)
          } yield RespValue.Integer(scoreMap.size.toLong - newSet.size.toLong)
        )

      case api.SortedSets.ZRevRank =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            rank = scoreMap.toArray.sortBy(_._2).reverse.map(_._1).indexOf(member) match {
                     case -1  => None
                     case idx => Some(idx)
                   }
          } yield rank.fold[RespValue](RespValue.NullBulkString)(result => RespValue.Integer(result.toLong))
        )

      case api.SortedSets.ZScore =>
        val key    = input(0).asString
        val member = input(1).asString

        orWrongType(isSortedSet(key))(
          for {
            scoreMap  <- sortedSets.getOrElse(key, Map.empty)
            maybeScore = scoreMap.get(member)
          } yield maybeScore.fold[RespValue](RespValue.NullBulkString)(result => RespValue.bulkString(result.toString))
        )

      case api.SortedSets.Zmscore =>
        val key     = input(0).asString
        val members = input.tail.map(_.asString)

        orWrongType(isSortedSet(key))(
          for {
            scoreMap   <- sortedSets.getOrElse(key, Map.empty)
            maybeScores = members.map(m => scoreMap.get(m))
            result = maybeScores.map {
                       case Some(v) => RespValue.bulkString(v.toString)
                       case None    => RespValue.NullBulkString
                     }
          } yield RespValue.array(result: _*)
        )

      case api.SortedSets.ZRange =>
        val key              = input.head.asString
        val start            = input(1).asString.toInt
        val end              = input(2).asString.toInt
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            slice =
              if (end < 0)
                scoreMap.toArray.sortBy(_._2).slice(start, scoreMap.size + 1 + end)
              else
                scoreMap.toArray.sortBy(_._2).slice(start, end + 1)

            result = withScoresOption.fold(slice.map(_._1))(_ =>
                       slice.flatMap { case (v, s) => Array(v, s.toString.stripSuffix(".0")) }
                     )
          } yield RespValue.Array(Chunk.fromIterable(result) map RespValue.bulkString)
        )

      case api.SortedSets.ZRevRange =>
        val key              = input.head.asString
        val start            = input(1).asString.toInt
        val end              = input(2).asString.toInt
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            slice =
              if (end < 0)
                scoreMap.toArray.sortBy(_._2).reverse.slice(start, scoreMap.size + 1 + end)
              else
                scoreMap.toArray.sortBy(_._2).reverse.slice(start, end + 1)

            result = withScoresOption.fold(slice.map(_._1))(_ =>
                       slice.flatMap { case (v, s) => Array(v, s.toString.stripSuffix(".0")) }
                     )
          } yield RespValue.Array(Chunk.fromIterable(result) map RespValue.bulkString)
        )

      case api.SortedSets.ZScan =>
        val key   = input.head.asString
        val start = input(1).asString.toInt

        val maybeRegex =
          if (input.size > 2) input(2).asString match {
            case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
            case _       => None
          }
          else None

        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val maybeCount =
          if (input.size > 4) maybeGetCount(input(4), input(5))
          else if (input.size > 2) maybeGetCount(input(2), input(3))
          else None

        val end = start + maybeCount.getOrElse(10)

        orWrongType(isSortedSet(key))(
          {
            for {
              scoreMap <- sortedSets.getOrElse(key, Map.empty)
              filtered =
                maybeRegex.map(regex => scoreMap.filter(s => regex.pattern.matcher(s._1).matches)).getOrElse(scoreMap)
              resultSet = filtered.toArray.sortBy(_._2).slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              expand    = resultSet.flatMap(v => Array(v._1, v._2.toString.stripSuffix(".0")))
              results   = Replies.array(expand)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
        )

      case api.SortedSets.ZUnionStore =>
        val destination = input(0).asString
        val numKeys     = input(1).asLong.toInt
        val keys        = input.drop(2).take(numKeys).map(_.asString)

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys :+ destination)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              unionKeys =
                sourceSets.map(_.keySet).reduce(_.union(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => unionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              destinationResult =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              _ <- putSortedSet(destination, destinationResult)
            } yield RespValue.Integer(destinationResult.size.toLong)
          )
        )

      case api.SortedSets.ZUnion =>
        val numKeys          = input(0).asLong.toInt
        val keys             = input.drop(1).take(numKeys).map(_.asString)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        val options = input.map(_.asString).zipWithIndex
        val aggregate =
          options
            .find(_._1 == "AGGREGATE")
            .map(_._2)
            .map(idx => input(idx + 1).asString)
            .map {
              case "SUM" => (_: Double) + (_: Double)
              case "MIN" => Math.min(_: Double, _: Double)
              case "MAX" => Math.max(_: Double, _: Double)
            }
            .getOrElse((_: Double) + (_: Double))

        val weights =
          options
            .find(_._1 == "WEIGHTS")
            .map(_._2)
            .map(idx =>
              input
                .drop(idx + 1)
                .takeWhile(v => Try(v.asString.stripSuffix(".0").toLong).isSuccess)
                .map(_.asString.stripSuffix(".0").toLong)
            )
            .getOrElse(Chunk.empty)

        orInvalidParameter(STM.succeed(!(weights.nonEmpty && weights.size != numKeys)))(
          orWrongType(forAll(keys)(isSortedSet))(
            for {
              sourceSets <- STM.foreach(keys)(key => sortedSets.getOrElse(key, Map.empty))

              unionKeys =
                sourceSets.map(_.keySet).reduce(_.union(_))

              weightedSets =
                sourceSets
                  .map(m => m.filter(m => unionKeys.contains(m._1)))
                  .zipAll(weights, Map.empty, 1L)
                  .map { case (scoreMap, weight) => scoreMap.map { case (member, score) => member -> score * weight } }

              unionMap =
                weightedSets
                  .flatMap(Chunk.fromIterable)
                  .groupBy(_._1)
                  .map { case (member, scores) => member -> scores.map(_._2).reduce(aggregate) }

              result =
                if (withScoresOption.isDefined)
                  Chunk.fromIterable(unionMap.toArray.sortBy(_._2).flatMap { case (v, s) =>
                    Chunk(bulkString(v), bulkString(s.toString))
                  })
                else
                  Chunk.fromIterable(unionMap.toArray.sortBy(_._2).map(e => bulkString(e._1)))

            } yield RespValue.Array(result)
          )
        )

      case api.SortedSets.ZRandMember =>
        val key              = input(0).asString
        val maybeCount       = input.tail.headOption.map(b => b.asString.toLong)
        val withScoresOption = input.map(_.asString).find(_ == "WITHSCORES")

        orWrongType(isSortedSet(key))(
          for {
            scoreMap <- sortedSets.getOrElse(key, Map.empty)
            asVector  = scoreMap.toVector
            res <- maybeCount match {
                     case None =>
                       selectOne(asVector, randomPick).map {
                         _.fold(RespValue.NullBulkString: RespValue)(s => RespValue.bulkString(s._1))
                       }

                     case Some(n) if n > 0 =>
                       selectN(asVector, n, randomPick).map[RespValue] { values =>
                         if (withScoresOption.isDefined) {
                           val flatMemberScores = values.flatMap { case (m, s) => m :: s.toString :: Nil }
                           if (flatMemberScores.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(flatMemberScores)
                         } else {
                           if (values.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(values.map(_._1))
                         }
                       }

                     case Some(n) if n < 0 =>
                       selectNWithReplacement(asVector, -n, randomPick).map[RespValue] { values =>
                         if (withScoresOption.isDefined) {
                           val flatMemeberScore = values.flatMap { case (m, s) => m :: s.toString :: Nil }
                           if (flatMemeberScore.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(flatMemeberScore)
                         } else {
                           if (values.isEmpty)
                             RespValue.NullArray
                           else
                             Replies.array(values.map(_._1))
                         }
                       }

                     case Some(_) => STM.succeedNow(RespValue.NullBulkString)
                   }
          } yield res
        )

      case _ => STM.succeedNow(RespValue.Error("ERR unknown command"))
    }
  }

  private[this] def orWrongType(predicate: USTM[Boolean])(
    program: => USTM[RespValue]
  ): USTM[RespValue] =
    STM.ifM(predicate)(program, STM.succeedNow(Replies.WrongType))

  private[this] def orInvalidParameter(predicate: USTM[Boolean])(
    program: => USTM[RespValue]
  ): USTM[RespValue] =
    STM.ifM(predicate)(program, STM.succeedNow(Replies.Error))

  private[this] def apply[A, B](optionAB: Option[A => B])(optionA: Option[A]): Option[B] = for {
    a  <- optionA
    ab <- optionAB
  } yield ab(a)

  private[this] def orMissingParameter[A](paramA: Option[A])(program: A => USTM[RespValue]): USTM[RespValue] =
    apply(Some(program))(paramA).getOrElse(STM.succeedNow(Replies.Error))

  private[this] def orMissingParameter2[A, B](paramA: Option[A], paramB: Option[B])(
    program: (A, B) => USTM[RespValue]
  ): USTM[RespValue] =
    apply(apply(Some(program.curried))(paramA))(paramB).getOrElse(STM.succeedNow(Replies.Error))

  private[this] def orMissingParameter3[A, B, C](paramA: Option[A], paramB: Option[B], paramC: Option[C])(
    program: (A, B, C) => USTM[RespValue]
  ): USTM[RespValue] =
    apply(apply(apply(Some(program.curried))(paramA))(paramB))(paramC).getOrElse(STM.succeedNow(Replies.Error))

  private[this] def orMissingParameter4[A, B, C, D](
    paramA: Option[A],
    paramB: Option[B],
    paramC: Option[C],
    paramD: Option[D]
  )(
    program: (A, B, C, D) => USTM[RespValue]
  ): USTM[RespValue] =
    apply(apply(apply(apply(Some(program.curried))(paramA))(paramB))(paramC))(paramD)
      .getOrElse(STM.succeedNow(Replies.Error))

  // check whether the key is a set or unused.
  private[this] def isSet(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.Sets))

  // Check whether the key is a string or unused.
  private[this] def isString(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.Strings))

  // check whether the key is a list or unused.
  private[this] def isList(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.Lists))

  // check whether the key is a hyperLogLog or unused.
  private[this] def isHyperLogLog(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.HyperLogs))

  // check whether the key is a hash or unused.
  private[this] def isHash(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.Hashes))

  // check whether the key is a hash or unused.
  private[this] def isSortedSet(name: String): USTM[Boolean] =
    keys.get(name).map(infoOpt => infoOpt.forall(i => i.`type` == KeyType.SortedSets))

  // Puts element into list and removes its expiration, if any.
  private[this] def putList(key: String, value: Chunk[String]): USTM[Unit] =
    lists.put(key, value) <* keys.put(key, KeyInfo(KeyType.Lists, None))

  // Puts element into set and removes its expiration, if any.
  private[this] def putSet(key: String, value: Set[String]): USTM[Unit] =
    sets.put(key, value) <* keys.put(key, KeyInfo(KeyType.Sets, None))

  // Saves string and removes its expiration, if any.
  private[this] def putString(key: String, value: String, expireAt: Option[Instant] = None): USTM[Unit] =
    strings.put(key, value) <* keys.put(key, KeyInfo(KeyType.Strings, expireAt))

  // Puts element into hyperLogLog and removes its expiration, if any.
  private[this] def putHyperLogLog(key: String, value: Set[String]): USTM[Unit] =
    hyperLogLogs.put(key, value) <* keys.put(key, KeyInfo(KeyType.HyperLogs, None))

  // Puts element into hash and removes its expiration, if any.
  private[this] def putHash(key: String, value: Map[String, String]): USTM[Unit] =
    hashes.put(key, value) <* keys.put(key, KeyInfo(KeyType.Hashes, None))

  // Puts element into set and removes its expiration, if any.
  private[this] def putSortedSet(key: String, value: Map[String, Double]): USTM[Unit] =
    sortedSets.put(key, value) <* keys.put(key, KeyInfo(KeyType.SortedSets, None))

  /**
   * Rename key by altering underlying data and metadata structures. Note: RENAME retains the data's expiration
   */
  def rename(key: String, newkey: String): STM[RespValue.Error, Unit] =
    for {
      keyInfoOpt <- keys.get(key)
      keyInfo    <- keyInfoOpt.fold[STM[RespValue.Error, KeyInfo]](STM.fail(Replies.Error))(STM.succeedNow)
      _          <- keys.delete(key)
      _          <- keys.put(newkey, keyInfo)
      _          <- STM.whenCaseM(lists.get(key)) { case Some(v) => lists.delete(key) *> lists.put(newkey, v) }
      _          <- STM.whenCaseM(sets.get(key)) { case Some(v) => sets.delete(key) *> sets.put(newkey, v) }
      _          <- STM.whenCaseM(strings.get(key)) { case Some(v) => strings.delete(key) *> strings.put(newkey, v) }
      _ <- STM.whenCaseM(hyperLogLogs.get(key)) { case Some(v) =>
             hyperLogLogs.delete(key) *> hyperLogLogs.put(newkey, v)
           }
      _ <- STM.whenCaseM(hashes.get(key)) { case Some(v) => hashes.delete(key) *> hashes.put(newkey, v) }
    } yield ()

  /** Deletes key from underlying data and metadata structures. */
  private[this] def delete(key: String): USTM[Int] =
    STM.ifM(keys.contains(key))(
      for {
        _ <- STM.whenM(isList(key))(lists.delete(key))
        _ <- STM.whenM(isSet(key))(sets.delete(key))
        _ <- STM.whenM(isString(key))(strings.delete(key))
        _ <- STM.whenM(isHyperLogLog(key))(hyperLogLogs.delete(key))
        _ <- STM.whenM(isHash(key))(hashes.delete(key))
        _ <- keys.delete(key)
      } yield 1,
      STM.succeedNow(0)
    )

  /** Deletes all expired keys */
  private def clearExpired(now: Instant): USTM[Unit] =
    keys.foreach { (key, info) =>
      val ttl = info.expireAt.map(e => Duration.fromInterval(now, e).toMillis).getOrElse(1L)
      if (ttl <= 0) delete(key).unit
      else STM.unit
    }

  /**
   * Returns the time-to-live of a key, if any.
   * @return
   *   `Duration` between the key's expiration and the current time.
   */
  private[this] def ttlOf(key: String, now: Instant): USTM[Option[Duration]] =
    keys.get(key).flatMap {
      case Some(KeyInfo(_, Some(expireAt))) => STM.some(Duration.fromInterval(now, expireAt))
      case _                                => STM.none
    }

  @tailrec
  private[this] def dropWhileLimit[A](xs: Chunk[A])(p: A => Boolean, k: Int): Chunk[A] =
    if (k <= 0 || xs.isEmpty || !p(xs.head)) xs
    else dropWhileLimit(xs.tail)(p, k - 1)

  private[this] def selectN[A](values: Vector[A], n: Long, pickRandom: Int => USTM[Int]): USTM[List[A]] = {
    def go(remaining: Vector[A], toPick: Long, acc: List[A]): USTM[List[A]] =
      (remaining, toPick) match {
        case (Vector(), _) | (_, 0) => STM.succeed(acc)
        case _ =>
          pickRandom(remaining.size).flatMap { index =>
            val x  = remaining(index)
            val xs = remaining.patch(index, Nil, 1)
            go(xs, toPick - 1, x :: acc)
          }
      }

    go(values, Math.max(n, 0), Nil)
  }

  private[this] def selectOne[A](values: Vector[A], pickRandom: Int => USTM[Int]): USTM[Option[A]] =
    if (values.isEmpty) STM.succeedNow(None)
    else pickRandom(values.size).map(index => Some(values(index)))

  private[this] def selectNWithReplacement[A](
    values: Vector[A],
    n: Long,
    pickRandom: Int => USTM[Int]
  ): USTM[List[A]] =
    STM
      .loop(Math.max(n, 0))(_ > 0, _ - 1)(_ => selectOne(values, pickRandom))
      .map(_.flatten)

  private[this] def sInter(mainKey: String, otherKeys: Chunk[String]): STM[Unit, Set[String]] = {
    sealed trait State
    object State {
      case object WrongType extends State

      final case class Continue(values: Set[String]) extends State
    }

    def get(key: String): STM[Nothing, State] =
      STM.ifM(isSet(key))(
        sets.get(key).map(_.fold[State](State.Continue(Set.empty))(State.Continue)),
        STM.succeedNow(State.WrongType)
      )

    def step(state: State, next: String): STM[Nothing, State] =
      state match {
        case State.WrongType => STM.succeedNow(State.WrongType)
        case State.Continue(values) =>
          get(next).map {
            case State.Continue(otherValues) =>
              val intersection = values.intersect(otherValues)
              State.Continue(intersection)
            case s => s
          }
      }

    for {
      init  <- get(mainKey)
      state <- STM.foldLeft(otherKeys)(init)(step)
      result <- state match {
                  case State.WrongType        => STM.fail(())
                  case State.Continue(values) => STM.succeedNow(values)
                }
    } yield result
  }

  private[this] object Hash {
    val longRange: (Double, Double) = (-180.0, 180.0)
    val latRange: (Double, Double)  = (-85.05112878, 85.05112878)

    def isValidLongLat(longLat: LongLat): Boolean =
      longLat.longitude >= longRange._1 && longLat.longitude <= longRange._2 && longLat.latitude >= latRange._1 && longLat.latitude <= latRange._2

    def encodeAsHash(
      longitude: Double,
      latitude: Double,
      longRange: (Double, Double) = longRange,
      latRange: (Double, Double) = latRange
    ): Long = {
      val longOffset = ((longitude - longRange._1) / (longRange._2 - longRange._1) * (1L << 26)).toLong
      val latOffset  = ((latitude - latRange._1) / (latRange._2 - latRange._1) * (1L << 26)).toLong

      @annotation.tailrec
      def findHash(
        acc: Long,
        bitPlace: Int
      ): Long =
        if (bitPlace < 0) acc
        else if (bitPlace % 2 == 0) findHash(acc | (latOffset & 1L << bitPlace / 2) << bitPlace / 2, bitPlace - 1)
        else findHash(acc | (longOffset & 1L << bitPlace / 2) << (bitPlace + 1) / 2, bitPlace - 1)

      findHash(0, 51)
    }

    def asGeoHash(hash: Long): String = {
      val base32       = "0123456789bcdefghjkmnpqrstuvwxyz"
      val longLat      = decodeHash(hash)
      val standardHash = encodeAsHash(longLat.longitude, longLat.latitude, latRange = (-90.0, 90.0))
      standardHash.toBinaryString.grouped(5).map(x => base32(Integer.parseInt(x, 2))).mkString.updated(10, base32(0))
    }

    def decodeHash(hash: Long): LongLat = {

      @annotation.tailrec
      def findLongLat(longBits: Long, latBits: Long, bitPlace: Long): (Long, Long) =
        if (bitPlace < 0) (longBits, latBits)
        else if (bitPlace % 2 == 0)
          findLongLat(longBits, latBits | (hash & 1L << bitPlace) >> (bitPlace / 2), bitPlace - 1)
        else findLongLat(longBits | (hash & 1L << bitPlace) >> (bitPlace / 2 + 1), latBits, bitPlace - 1)

      val (longBits, latBits) = findLongLat(0L, 0L, 51)

      val longDiff = longRange._2 - longRange._1
      val latDiff  = latRange._2 - latRange._1

      val longLow  = longRange._1 + longBits * 1.0 / (1L << 26) * longDiff
      val longHigh = longRange._1 + (longBits + 1.0) * 1.0 / (1L << 26) * longDiff
      val latLow   = latRange._1 + latBits * 1.0 / (1L << 26) * latDiff
      val latHigh  = latRange._1 + (latBits + 1.0) * 1.0 / (1L << 26) * latDiff
      LongLat((longLow + longHigh) / 2.0, (latLow + latHigh) / 2.0)
    }

    def distance(longLat1: LongLat, longLat2: LongLat, unit: RadiusUnit = RadiusUnit.Meters): Double = {
      import Math._

      val latDiff  = (longLat2.latitude - longLat1.latitude).toRadians
      val longDiff = (longLat2.longitude - longLat1.longitude).toRadians
      val lat1     = longLat1.latitude.toRadians
      val lat2     = longLat2.latitude.toRadians
      val toMeters = unit match {
        case RadiusUnit.Meters     => 1.0
        case RadiusUnit.Kilometers => 1000.0
        case RadiusUnit.Feet       => 0.3048
        case RadiusUnit.Miles      => 1609.34
      }

      val u        = sin(latDiff / 2.0)
      val v        = sin(longDiff / 2.0)
      val radius   = 6372797.560856 / toMeters
      val distance = 2.0 * radius * asin(sqrt(u * u + cos(lat1) * cos(lat2) * v * v))

      round(distance * 10000.0) / 10000.0
    }
  }

  private[this] def forAll[A](chunk: Chunk[A])(f: A => STM[Nothing, Boolean]) =
    STM.foldLeft(chunk)(true) { case (b, a) => f(a).map(b && _) }

  private[this] object Replies {
    val Ok: RespValue.SimpleString = RespValue.SimpleString("OK")
    val WrongType: RespValue.Error = RespValue.Error("WRONGTYPE")
    val Error: RespValue.Error     = RespValue.Error("ERR")

    def array(values: Iterable[String]): RespValue.Array =
      RespValue.array(values.map(RespValue.bulkString).toList: _*)

    val EmptyArray: RespValue.Array = RespValue.array()
  }

}

private[redis] object TestExecutor {

  sealed trait KeyType extends Product with Serializable

  object KeyType {
    case object Strings    extends KeyType
    case object HyperLogs  extends KeyType
    case object Sets       extends KeyType
    case object Lists      extends KeyType
    case object Hashes     extends KeyType
    case object SortedSets extends KeyType
    case object Stream     extends KeyType

    def toRedisType(keyType: KeyType): RedisType = keyType match {
      case Strings    => RedisType.String
      case HyperLogs  => RedisType.String
      case Sets       => RedisType.Set
      case Lists      => RedisType.List
      case Hashes     => RedisType.Hash
      case SortedSets => RedisType.SortedSet
      case Stream     => RedisType.Stream
    }
  }

  final case class KeyInfo(`type`: KeyType, expireAt: Option[Instant]) {
    lazy val redisType: RedisType = KeyType.toRedisType(`type`)
  }

  lazy val live: URLayer[zio.random.Random with Clock, RedisExecutor] = {
    val executor = for {
      seed         <- random.nextInt
      clock        <- ZIO.identity[Clock]
      sRandom       = new scala.util.Random(seed)
      ref          <- TRef.make(LazyList.continually((i: Int) => sRandom.nextInt(i))).commit
      randomPick    = (i: Int) => ref.modify(s => (s.head(i), s.tail))
      keys         <- TMap.empty[String, KeyInfo].commit
      sets         <- TMap.empty[String, Set[String]].commit
      strings      <- TMap.empty[String, String].commit
      hyperLogLogs <- TMap.empty[String, Set[String]].commit
      lists        <- TMap.empty[String, Chunk[String]].commit
      hashes       <- TMap.empty[String, Map[String, String]].commit
      sortedSets   <- TMap.empty[String, Map[String, Double]].commit
      clientInfo   <- TRef.make(ClientInfo(id = 174716)).commit
      clientTInfo =
        ClientTrackingInfo(ClientTrackingFlags(clientSideCaching = false), ClientTrackingRedirect.NotEnabled)
      clientTrackingInfo <- TRef.make(clientTInfo).commit
    } yield new TestExecutor(
      clientInfo,
      clientTrackingInfo,
      keys,
      lists,
      sets,
      strings,
      randomPick,
      hyperLogLogs,
      hashes,
      sortedSets,
      clock
    )

    executor.toLayer
  }

}
