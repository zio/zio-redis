
package zio.redis

import java.time.Instant

import scala.annotation.tailrec
import scala.collection.compat.immutable.LazyList
import scala.util.Try

import zio._
import zio.clock.Clock
import zio.duration._
import zio.redis.RedisError.ProtocolError
import zio.redis.RespValue.BulkString
import zio.redis.codec.StringUtf8Codec
import zio.schema.codec.Codec
import zio.stm.{ random => _, _ }

private[redis] final class TestExecutor private (
  keys: TSet[String],
  lists: TMap[String, Chunk[String]],
  sets: TMap[String, Set[String]],
  strings: TMap[String, String],
  randomPick: Int => USTM[Int],
  hyperLogLogs: TMap[String, Set[String]],
  hashes: TMap[String, Map[String, String]],
  expirations: TMap[String, Instant],
  testClock: Clock
) extends RedisExecutor.Service {

  override val codec: Codec = StringUtf8Codec

  override def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
    for {
      name <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
      now  <- testClock.get.instant
      _    <- clearExpired(now)
      result <- name.asString match {
                  case api.Lists.BlPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray, now)
                      .provide(testClock)

                  case api.Lists.BrPop =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullArray, now)
                      .provide(testClock)

                  case api.Lists.BrPopLPush =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)
                      .provide(testClock)

                  case api.Lists.BlMove =>
                    val timeout = command.tail.last.asString.toInt
                    runBlockingCommand(name.asString, command.tail, timeout, RespValue.NullBulkString, now)
                      .provide(testClock)

                  case _ => runCommand(name.asString, command.tail, now).commit
                }
    } yield result

  private def runBlockingCommand(
    name: String,
    input: Chunk[RespValue.BulkString],
    timeout: Int,
    respValue: RespValue,
    now: Instant
  ): URIO[Clock, RespValue] =
    if (timeout > 0)
      runCommand(name, input, now).commit
        .timeout(timeout.seconds)
        .map(_.getOrElse(respValue))
    else
      runCommand(name, input, now).commit

  private def errResponse(cmd: String): RespValue.BulkString =
    RespValue.bulkString(s"(error) ERR wrong number of arguments for '$cmd' command")

  private def onConnection(command: String, input: Chunk[RespValue.BulkString])(
    res: => RespValue.BulkString
  ): USTM[BulkString] = STM.succeedNow(if (input.isEmpty) errResponse(command) else res)

  private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString], now: Instant): USTM[RespValue] = {
    name match {
      case api.Connection.Ping.name =>
        STM.succeedNow {
          if (input.isEmpty)
            RespValue.bulkString("PONG")
          else
            input.head
        }

      case api.Connection.Auth.name =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Connection.Echo.name =>
        onConnection(name, input)(input.head)

      case api.Connection.Select.name =>
        onConnection(name, input)(RespValue.bulkString("OK"))

      case api.Keys.Exists =>
        val allkeys = input.map(_.asString)
        STM
          .foldLeft(allkeys)(0L) { case (acc, key) =>
            STM.ifM(keys.contains(key))(STM.succeedNow(acc + 1), STM.succeedNow(acc))
          }
          .map(RespValue.Integer)

      case api.Keys.Del =>
        val allkeys = input.map(_.asString)
        STM
          .foldLeft(allkeys)(0L) { case (acc, key) => delete(key).map(acc + _) }
          .map(RespValue.Integer)

      case api.Keys.TypeOf =>
        val key = input.head.asString
        val matchedType = for {
          a <- STM.ifM(isList(key))(STM.some(RedisType.List), STM.none)
          b <- STM.ifM(isSet(key))(STM.some(RedisType.Set), STM.none)
          c <- STM.ifM(isString(key))(STM.some(RedisType.String), STM.none)
          d <- STM.ifM(isHyperLogLog(key))(STM.some(RedisType.String), STM.none)
          e <- STM.ifM(isHash(key))(STM.some(RedisType.Hash), STM.none)
        } yield a orElse b orElse c orElse d orElse e
        matchedType.map(_.fold("none")(_.stringify)).map(RespValue.SimpleString)

      case api.Keys.RandomKey =>
        for {
          ks   <- keys.toList
          pick <- selectOne(ks.toVector, randomPick)
        } yield pick.fold(RespValue.NullBulkString: RespValue)(RespValue.bulkString)

      case api.Keys.Ttl =>
        val key = input.head.asString
        STM
          .ifM(keys.contains(key))(
            ttlOf(key, now).map(_.fold(-1L)(_.toSeconds)),
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
        expirations
          .get(key)
          .flatMap(_.fold(STM.succeedNow(0L))(_ => expirations.delete(key).as(1L)))
          .map(RespValue.Integer)

      case api.Keys.Expire =>
        val key      = input(0).asString
        val unixtime = now.plusSeconds(input(1).asLong)
        STM
          .ifM(keys.contains(key))(expirations.put(key, unixtime).as(1L), STM.succeedNow(0L))
          .map(RespValue.Integer)

      case api.Keys.ExpireAt =>
        val key      = input(0).asString
        val unixtime = Instant.ofEpochSecond(input(1).asLong)
        STM
          .ifM(keys.contains(key))(expirations.put(key, unixtime).as(1L), STM.succeedNow(0L))
          .map(RespValue.Integer)

      case api.Keys.PExpire =>
        val key      = input(0).asString
        val unixtime = now.plusMillis(input(1).asLong)
        STM
          .ifM(keys.contains(key))(expirations.put(key, unixtime).as(1L), STM.succeedNow(0L))
          .map(RespValue.Integer)

      case api.Keys.PExpireAt =>
        val key      = input(0).asString
        val unixtime = Instant.ofEpochMilli(input(1).asLong)
        STM
          .ifM(keys.contains(key))(expirations.put(key, unixtime).as(1L), STM.succeedNow(0L))
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
        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val key = input.head.asString

        orWrongType(isSet(key))(
          {
            val start = input(1).asString.toInt
            val maybeRegex = if (input.size > 2) input(2).asString match {
              case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
              case _       => None
            }
            else None
            val maybeCount =
              if (input.size > 4) maybeGetCount(input(4), input(5))
              else if (input.size > 2) maybeGetCount(input(2), input(3))
              else None
            val end = start + maybeCount.getOrElse(10)
            for {
              set      <- sets.getOrElse(key, Set.empty)
              filtered  = maybeRegex.map(regex => set.filter(s => regex.pattern.matcher(s).matches)).getOrElse(set)
              resultSet = filtered.slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              results   = Replies.array(resultSet)
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
        )

      case api.Strings.Set =>
        // not a full implementation. Just enough to make set tests work
        val key   = input.head.asString
        val value = input(1).asString
        orWrongType(isString(key))(
          putString(key, value).as(Replies.Ok)
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
            _ <- if (newHash.isEmpty) delete(key)
                 else putHash(key, newHash)
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
        def maybeGetCount(key: RespValue.BulkString, value: RespValue.BulkString): Option[Int] =
          key.asString match {
            case "COUNT" => Some(value.asString.toInt)
            case _       => None
          }

        val key = input.head.asString

        orWrongType(isHash(key))(
          {
            val start = input(1).asString.toInt
            val maybeRegex = if (input.size > 2) input(2).asString match {
              case "MATCH" => Some(input(3).asString.replace("*", ".*").r)
              case _       => None
            }
            else None
            val maybeCount =
              if (input.size > 4) maybeGetCount(input(4), input(5))
              else if (input.size > 2) maybeGetCount(input(2), input(3))
              else None
            val end = start + maybeCount.getOrElse(10)
            for {
              set <- hashes.getOrElse(key, Map.empty)
              filtered =
                maybeRegex.map(regex => set.filter { case (k, _) => regex.pattern.matcher(k).matches }).getOrElse(set)
              resultSet = filtered.slice(start, end)
              nextIndex = if (filtered.size <= end) 0 else end
              results   = Replies.array(resultSet.flatMap { case (k, v) => Iterable(k, v) })
            } yield RespValue.array(RespValue.bulkString(nextIndex.toString), results)
          }
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

      case _ => STM.succeedNow(RespValue.Error("ERR unknown command"))
    }
  }

  /** Removes all expired keys from each data structure */
  private def clearExpired(now: Instant): UIO[Unit] =
    expirations.foreach { (key, exp) =>
      if (now.isBefore(exp)) STM.unit
      else delete(key).unit
    }.commit

  private[this] def delete(key: String): USTM[Int] =
    STM.ifM(keys.contains(key))(
      for {
        _ <- STM.whenM(isList(key))(lists.delete(key))
        _ <- STM.whenM(isSet(key))(sets.delete(key))
        _ <- STM.whenM(isString(key))(strings.delete(key))
        _ <- STM.whenM(isHyperLogLog(key))(hyperLogLogs.delete(key))
        _ <- STM.whenM(isHash(key))(hashes.delete(key))
        _ <- keys.delete(key)
        _ <- expirations.delete(key)
      } yield 1,
      STM.succeedNow(0)
    )

  private[this] def orWrongType(predicate: USTM[Boolean])(
    program: => USTM[RespValue]
  ): USTM[RespValue] =
    STM.ifM(predicate)(program, STM.succeedNow(Replies.WrongType))

  // check whether the key is a set or unused.
  private[this] def isSet(name: String): STM[Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isList   <- lists.contains(name)
      isHyper  <- hyperLogLogs.contains(name)
      isHash   <- hashes.contains(name)
    } yield !isString && !isList && !isHyper && !isHash

  // Check whether the key is a string or unused.
  private[this] def isString(name: String): USTM[Boolean] =
    for {
      isSet   <- sets.contains(name)
      isList  <- lists.contains(name)
      isHyper <- hyperLogLogs.contains(name)
      isHash  <- hashes.contains(name)
    } yield !isSet && !isList && !isHyper && !isHash

  // check whether the key is a list or unused.
  private[this] def isList(name: String): STM[Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isSet    <- sets.contains(name)
      isHyper  <- hyperLogLogs.contains(name)
      isHash   <- hashes.contains(name)
    } yield !isString && !isSet && !isHyper && !isHash

  //check whether the key is a hyperLogLog or unused.
  private[this] def isHyperLogLog(name: String): ZSTM[Any, Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isSet    <- sets.contains(name)
      isList   <- lists.contains(name)
      isHash   <- hashes.contains(name)
    } yield !isString && !isSet && !isList && !isHash

  //check whether the key is a hash or unused.
  private[this] def isHash(name: String): ZSTM[Any, Nothing, Boolean] =
    for {
      isString <- strings.contains(name)
      isSet    <- sets.contains(name)
      isList   <- lists.contains(name)
      isHyper  <- hyperLogLogs.contains(name)
    } yield !isString && !isSet && !isList && !isHyper

  // Puts element into list and removes its expiration, if any.
  private[this] def putList(key: String, value: Chunk[String]): USTM[Unit] =
    lists.put(key, value) <* keys.put(key) <* expirations.delete(key)

  // Puts element into set and removes its expiration, if any.
  private[this] def putSet(key: String, value: Set[String]): USTM[Unit] =
    sets.put(key, value) <* keys.put(key) <* expirations.delete(key)

  // Saves string and removes its expiration, if any.
  private[this] def putString(key: String, value: String): USTM[Unit] =
    strings.put(key, value) <* keys.put(key) <* expirations.delete(key)

  // Puts element into hyperLogLog and removes its expiration, if any.
  private[this] def putHyperLogLog(key: String, value: Set[String]): USTM[Unit] =
    hyperLogLogs.put(key, value) <* keys.put(key) <* expirations.delete(key)

  // Puts element into hash and removes its expiration, if any.
  private[this] def putHash(key: String, value: Map[String, String]): USTM[Unit] =
    hashes.put(key, value) <* keys.put(key) <* expirations.delete(key)

  /**
   * Returns the time-to-live of a key, if any.
   *  @return `Duration` between the key's expiration and the current time.
   */
  private[this] def ttlOf(key: String, now: Instant): USTM[Option[Duration]] =
    expirations.get(key).flatMap {
      case Some(exp) => STM.some(Duration.fromInterval(exp, now))
      case None      => STM.none
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

      case object Empty extends State

      final case class Continue(values: Set[String]) extends State

    }
    def get(key: String): STM[Nothing, State] =
      STM.ifM(isSet(key))(
        sets.get(key).map(_.fold[State](State.Empty)(State.Continue)),
        STM.succeedNow(State.WrongType)
      )

    def step(state: State, next: String): STM[Nothing, State] =
      state match {
        case State.Empty     => STM.succeedNow(State.Empty)
        case State.WrongType => STM.succeedNow(State.WrongType)
        case State.Continue(values) =>
          get(next).map {
            case State.Continue(otherValues) =>
              val intersection = values.intersect(otherValues)
              if (intersection.isEmpty) State.Empty else State.Continue(intersection)
            case s => s
          }
      }

    for {
      init  <- get(mainKey)
      state <- STM.foldLeft(otherKeys)(init)(step)
      result <- state match {
                  case State.Continue(values) => STM.succeedNow(values)
                  case State.Empty            => STM.succeedNow(Set.empty[String])
                  case State.WrongType        => STM.fail(())
                }
    } yield result
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
  lazy val live: URLayer[zio.random.Random with Clock, RedisExecutor] = {
    val executor = for {
      seed         <- random.nextInt
      sRandom       = new scala.util.Random(seed)
      ref          <- TRef.make(LazyList.continually((i: Int) => sRandom.nextInt(i))).commit
      randomPick    = (i: Int) => ref.modify(s => (s.head(i), s.tail))
      keys         <- TSet.empty[String].commit
      sets         <- TMap.empty[String, Set[String]].commit
      strings      <- TMap.empty[String, String].commit
      hyperLogLogs <- TMap.empty[String, Set[String]].commit
      lists        <- TMap.empty[String, Chunk[String]].commit
      hashes       <- TMap.empty[String, Map[String, String]].commit
      expirations  <- TMap.empty[String, Instant].commit
      testClock    <- ZIO.identity[Clock]
    } yield new TestExecutor(keys, lists, sets, strings, randomPick, hyperLogLogs, hashes, expirations, testClock)

    executor.toLayer
  }

}

