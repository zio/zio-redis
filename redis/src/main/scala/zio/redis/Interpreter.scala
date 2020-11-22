package zio.redis

import java.io.IOException
import java.net.SocketAddress

import zio._
import zio.logging._
import zio.redis.RedisError.ProtocolError
import zio.stm._
import zio.stream.Stream

trait Interpreter {

  import Interpreter._

  type RedisExecutor = Has[RedisExecutor.Service]

  object RedisExecutor {
    trait Service {
      def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue]
    }

    def live(address: => SocketAddress): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.live(address)) >>> StreamedExecutor

    def live(host: String, port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.live(host, port)) >>> StreamedExecutor

    def loopback(port: Int = DefaultPort): ZLayer[Logging, RedisError.IOError, RedisExecutor] =
      (ZLayer.identity[Logging] ++ ByteStream.loopback(port)) >>> StreamedExecutor

    val test: ZLayer[Any, Nothing, RedisExecutor] =
      ZLayer.fromEffect {
        STM.atomically {
          for {
            sets    <- TMap.empty[String, Set[String]]
            strings <- TMap.empty[String, String]
          } yield new Test(sets, strings)
        }
      }

    private[redis] final val DefaultPort = 6379

    private[this] final val RequestQueueSize = 16

    private[this] final val StreamedExecutor =
      ZLayer.fromServicesManaged[ByteStream.Service, Logger[String], Any, RedisError.IOError, RedisExecutor.Service] {
        (byteStream: ByteStream.Service, logging: Logger[String]) =>
          for {
            reqQueue <- Queue.bounded[Request](RequestQueueSize).toManaged_
            resQueue <- Queue.unbounded[Promise[RedisError, RespValue]].toManaged_
            live      = new Live(reqQueue, resQueue, byteStream, logging)
            _        <- live.run.forkManaged
          } yield live
      }

    private[this] final class Live(
      reqQueue: Queue[Request],
      resQueue: Queue[Promise[RedisError, RespValue]],
      byteStream: ByteStream.Service,
      logger: Logger[String]
    ) extends Service {

      def execute(command: Chunk[RespValue.BulkString]): IO[RedisError, RespValue] =
        Promise
          .make[RedisError, RespValue]
          .flatMap(promise => reqQueue.offer(Request(command, promise)) *> promise.await)

      /**
       * Opens a connection to the server and launches send and receive operations.
       * All failures are retried by opening a new connection.
       * Only exits by interruption or defect.
       */
      val run: IO[RedisError, Unit] =
        (send.forever race runReceive(byteStream.read))
          .tapError(e => logger.warn(s"Reconnecting due to error: $e") *> drainWith(e))
          .retryWhile(True)
          .tapError(e => logger.error(s"Executor exiting: $e"))

      private def drainWith(e: RedisError): UIO[Unit] = resQueue.takeAll.flatMap(IO.foreach_(_)(_.fail(e)))

      private def send: IO[RedisError, Unit] =
        reqQueue.takeBetween(1, Int.MaxValue).flatMap { reqs =>
          val bytes = Chunk.fromIterable(reqs).flatMap(req => RespValue.Array(req.command).serialize)
          byteStream
            .write(bytes)
            .mapError(RedisError.IOError)
            .tapBoth(
              e => IO.foreach_(reqs)(_.promise.fail(e)),
              _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
            )
        }

      private def runReceive(inStream: Stream[IOException, Byte]): IO[RedisError, Unit] =
        inStream
          .mapError(RedisError.IOError)
          .transduce(RespValue.Deserializer.toTransducer)
          .foreach(response => resQueue.take.flatMap(_.succeed(response)))

    }

    private final class Test(
      sets: TMap[String, Set[String]],
      strings: TMap[String, String]
    ) extends Service {
      override def execute(command: Chunk[RespValue.BulkString]): zio.IO[RedisError, RespValue] =
        for {
          name   <- ZIO.fromOption(command.headOption).orElseFail(ProtocolError("Malformed command."))
          result <- runCommand(name.asString, command.tail).commit
        } yield result

      private[this] def runCommand(name: String, input: Chunk[RespValue.BulkString]): STM[RedisError, RespValue] =
        name match {
          case api.Connection.Ping.name  =>
            STM.succeedNow {
              if (input.isEmpty)
                RespValue.bulkString("PONG")
              else
                input.head
            }
          case api.Sets.SAdd.name        =>
            val key = input.head.asString
            STM.ifM(isSet(key))(
              {
                val values = input.tail.map(_.asString)
                for {
                  oldSet <- sets.getOrElse(key, Set.empty)
                  newSet  = oldSet ++ values
                  added   = newSet.size - oldSet.size
                  _      <- sets.put(key, newSet)
                } yield RespValue.Integer(added.toLong)
              },
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SCard.name       =>
            val key = input.head.asString
            STM.ifM(isSet(key))(
              sets.get(key).map(_.fold(RespValue.Integer(0))(s => RespValue.Integer(s.size.toLong))),
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SDiff.name       =>
            val allkeys = input.map(_.asString)
            val mainKey = allkeys.head
            val others  = allkeys.tail
            STM.ifM(forAll(allkeys)(isSet))(
              for {
                main   <- sets.getOrElse(mainKey, Set.empty)
                result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
              } yield RespValue.array(result.map(RespValue.bulkString(_)).toList: _*),
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SDiffStore.name  =>
            val allkeys = input.map(_.asString)
            val distkey = allkeys.head
            val mainKey = allkeys(1)
            val others  = allkeys.drop(2)
            STM.ifM(forAll(allkeys)(isSet))(
              for {
                main   <- sets.getOrElse(mainKey, Set.empty)
                result <- STM.foldLeft(others)(main) { case (acc, k) => sets.get(k).map(_.fold(acc)(acc -- _)) }
                _      <- sets.put(distkey, result)
              } yield RespValue.Integer(result.size.toLong),
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SInter.name      =>
            val keys      = input.map(_.asString)
            val mainKey   = keys.head
            val otherKeys = keys.tail
            sInter(mainKey, otherKeys).fold(_ => Replies.WrongType, Replies.array)
          case api.Sets.SInterStore.name =>
            val keys        = input.map(_.asString)
            val destination = keys.head
            val mainKey     = keys(1)
            val otherKeys   = keys.tail
            (STM.fail(()).unlessM(isSet(destination)) *> sInter(mainKey, otherKeys)).foldM(
              _ => STM.succeedNow(Replies.WrongType),
              s =>
                for {
                  _ <- sets.put(destination, s)
                } yield RespValue.Integer(s.size.toLong)
            )
          case api.Sets.SIsMember.name   =>
            val key    = input.head.asString
            val member = input(1).asString
            STM.ifM(isSet(key))(
              for {
                set   <- sets.getOrElse(key, Set.empty)
                result = if (set.contains(member)) RespValue.Integer(1) else RespValue.Integer(0)
              } yield result,
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SMove.name       =>
            val sourceKey      = input.head.asString
            val destinationKey = input(1).asString
            val member         = input(2).asString
            STM.ifM(isSet(sourceKey))(
              sets.getOrElse(sourceKey, Set.empty).flatMap { source =>
                if (source.contains(member))
                  STM.ifM(isSet(destinationKey))(
                    for {
                      dest <- sets.getOrElse(destinationKey, Set.empty)
                      _    <- sets.put(sourceKey, source - member)
                      _    <- sets.put(destinationKey, dest + member)
                    } yield RespValue.Integer(1),
                    STM.succeedNow(Replies.WrongType)
                  )
                else STM.succeedNow(RespValue.Integer(0))
              },
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SPop.name        =>
            val key   = input.head.asString
            val count = if (input.size == 1) 1 else input(1).asString.toInt
            STM.ifM(isSet(key))(
              for {
                set   <- sets.getOrElse(key, Set.empty)
                result = set.take(count)
                _     <- sets.put(key, set -- result)
              } yield Replies.array(result),
              STM.succeedNow(Replies.WrongType)
            )
          case api.Sets.SMembers.name    =>
            val key = input.head.asString
            STM.ifM(isSet(key))(
              sets.get(key).map(_.fold(Replies.EmptyArray)(Replies.array(_))),
              STM.succeedNow(Replies.WrongType)
            )
          case api.Strings.Set.name      =>
            // not a full implementation. Just enough to make set tests work
            val key   = input.head.asString
            val value = input(1).asString
            strings.put(key, value).as(Replies.Ok)

          case _                         => STM.fail(RedisError.ProtocolError(s"Command not supported by test executor: $name"))
        }

      // check whether the key is a set or unused.
      private[this] def isSet(name: String): STM[Nothing, Boolean] =
        for {
          isString <- strings.contains(name)
        } yield !isString

      private[this] def sInter(mainKey: String, otherKeys: Chunk[String]): STM[Unit, Set[String]] = {
        sealed trait State
        object State {
          case object WrongType                          extends State
          case object Empty                              extends State
          final case class Continue(values: Set[String]) extends State
        }
        def get(key: String): STM[Nothing, State] =
          STM.ifM(isSet(key))(
            sets.get(key).map(_.fold[State](State.Empty)(State.Continue(_))),
            STM.succeedNow(State.WrongType)
          )

        def step(state: State, next: String): STM[Nothing, State] =
          state match {
            case State.Empty            => STM.succeedNow(State.Empty)
            case State.WrongType        => STM.succeedNow(State.WrongType)
            case State.Continue(values) =>
              get(next).map {
                case State.Continue(otherValues) =>
                  val intersection = values.intersect(otherValues)
                  if (intersection.isEmpty) State.Empty else State.Continue(intersection)
                case s                           => s
              }
          }

        for {
          init   <- get(mainKey)
          state  <- STM.foldLeft(otherKeys)(init)(step)
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
        val Ok                              = RespValue.SimpleString("OK")
        val WrongType                       = RespValue.Error("WRONGTYPE")
        def array(values: Iterable[String]) =
          RespValue.array(values.map(RespValue.bulkString(_)).toList: _*)
        val EmptyArray                      = RespValue.array()
      }
    }
  }
}

private[redis] object Interpreter {
  private final case class Request(command: Chunk[RespValue.BulkString], promise: Promise[RedisError, RespValue])

  private val True: Any => Boolean = _ => true
}
