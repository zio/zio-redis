package zio.redis

import java.net.InetAddress

import zio.duration._
import zio.redis.Output._
import zio.redis.RedisError._
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object OutputSpec extends BaseSpec {

  private def respBulkString(s: String) = RespValue.bulkString(s)

  private def respDouble(d: Double) = respBulkString(d.toString)

  private def respArray(xs: String*) = RespValue.Array(Chunk.fromIterable(xs.map(respBulkString)))

  private def respArrayVals(xs: RespValue*) = RespValue.Array(Chunk.fromIterable(xs))

  private def respInt(i: Long) = RespValue.Integer(i)

  def spec =
    suite("Output decoders")(
      suite("errors")(
        testM("protocol errors") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Error(s"ERR $Noise"))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(Noise))))
        },
        testM("wrong type") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Error(s"WRONGTYPE $Noise"))).either
          } yield assert(res)(isLeft(equalTo(WrongType(Noise))))
        }
      ),
      suite("boolean")(
        testM("extract true") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Integer(1L)))
          } yield assert(res)(isTrue)
        },
        testM("extract false") {
          for {
            res <- Task(BoolOutput.unsafeDecode(RespValue.Integer(0)))
          } yield assert(res)(isFalse)
        }
      ),
      suite("chunk")(
        testM("extract empty arrays") {
          for {
            res <- Task(ChunkOutput.unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        testM("extract non-empty arrays") {
          for {
            res <- Task(ChunkOutput.unsafeDecode(respArray("foo", "bar")))
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        }
      ),
      suite("double")(
        testM("extract numbers") {
          val num = 42.3
          for {
            res <- Task(DoubleOutput.unsafeDecode(respDouble(num)))
          } yield assert(res)(equalTo(num))
        },
        testM("report number format exceptions as protocol errors") {
          val bad = "ok"
          for {
            res <- Task(DoubleOutput.unsafeDecode(respBulkString(bad))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"'$bad' isn't a double."))))
        }
      ),
      suite("durations")(
        suite("milliseconds")(
          testM("extract milliseconds") {
            val num = 42L
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.millis))
          },
          testM("report error for non-existing key") {
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            for {
              res <- Task(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        ),
        suite("seconds")(
          testM("extract seconds") {
            val num = 42L
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.seconds))
          },
          testM("report error for non-existing key") {
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          testM("report error for key without TTL") {
            for {
              res <- Task(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        )
      ),
      suite("long")(
        testM("extract positive numbers") {
          val num = 42L
          for {
            res <- Task(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        },
        testM("extract negative numbers") {
          val num = -42L
          for {
            res <- Task(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        }
      ),
      suite("optional")(
        testM("extract None") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(RespValue.NullValue))
          } yield assert(res)(isNone)
        },
        testM("extract some") {
          for {
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isSome(isUnit))
        }
      ),
      suite("scan")(
        testM("extract cursor and elements") {
          val input = respArrayVals(respBulkString("5"), respArray("foo", "bar"))
          for {
            res <- Task(ScanOutput.unsafeDecode(input))
          } yield assert(res)(equalTo("5" -> Chunk("foo", "bar")))
        }
      ),
      suite("string")(
        testM("extract strings") {
          for {
            res <- Task(MultiStringOutput.unsafeDecode(respBulkString(Noise)))
          } yield assert(res)(equalTo(Noise))
        }
      ),
      suite("unit")(
        testM("extract unit") {
          for {
            res <- Task(UnitOutput.unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isUnit)
        }
      ),
      suite("keyElem")(
        testM("extract none") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode(RespValue.NullValue))
          } yield assert(res)(isNone)
        },
        testM("extract key and element") {
          for {
            res <- Task(KeyElemOutput.unsafeDecode(respArray("key", "a")))
          } yield assert(res)(isSome(equalTo(("key", "a"))))
        },
        testM("report invalid input as protocol error") {
          val input = respArray("1", "2", "3")
          for {
            res <- Task(KeyElemOutput.unsafeDecode(input)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("multiStringChunk")(
        testM("extract one empty value") {
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(RespValue.NullValue))
          } yield assert(res)(isEmpty)
        },
        testM("extract one multi-string value") {
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(respBulkString("ab")))
          } yield assert(res)(hasSameElements(Chunk("ab")))
        },
        testM("extract one array value") {
          val input = respArray("1", "2", "3")
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(input))
          } yield assert(res)(hasSameElements(Chunk("1", "2", "3")))
        }
      ),
      suite("chunkOptionalMultiString")(
        testM("extract one empty value") {
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(RespValue.NullValue))
          } yield assert(res)(isEmpty)
        },
        testM("extract array with one non-empty element") {
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(respArray("ab")))
          } yield assert(res)(equalTo(Chunk(Some("ab"))))
        },
        testM("extract array with multiple non-empty elements") {
          val input = respArray("1", "2", "3")
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), Some("2"), Some("3"))))
        },
        testM("extract array with empty and non-empty elements") {
          val input = respArrayVals(respBulkString("1"), RespValue.NullValue, respBulkString("3"))
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), None, Some("3"))))
        }
      ),
      suite("chunkOptionalLong")(
        testM("extract one empty value") {
          for {
            res <- Task(ChunkOptionalLongOutput.unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        testM("extract array with empty and non-empty elements") {
          val input = respArrayVals(
            RespValue.Integer(1L),
            RespValue.NullValue,
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- Task(ChunkOptionalLongOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), None, Some(2L), Some(3L))))
        },
        testM("extract array with non-empty elements") {
          val input = respArrayVals(
            RespValue.Integer(1L),
            RespValue.Integer(1L),
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- Task(ChunkOptionalLongOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), Some(1L), Some(2L), Some(3L))))
        }
      ),
      suite("ClientInfo")(
        testM("addresses") {
          val id          = 42L
          val inetAddress = InetAddress.getByName("127.0.0.1")
          val input       = respBulkString(s"addr=${inetAddress.getHostAddress} id=$id laddr=${inetAddress.getHostAddress}")
          for {
            res <- Task(ClientInfoOutput.unsafeDecode(input))
          } yield assert(res)(
            equalTo(Chunk.single(ClientInfo(id = id, address = Some(inetAddress), localAddress = Some(inetAddress))))
          )
        },
        testM("flags") {
          import ClientFlag._
          val id                             = 42L
          val input                          = respBulkString(s"flags=bOPSRt id=$id")
          val expectedFlags: Set[ClientFlag] = Set(
            Blocked,
            MonitorMode,
            PubSub,
            Replica,
            TrackingTargetClientInvalid,
            KeysTrackingEnabled
          )
          for {
            res <- Task(ClientInfoOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk.single(ClientInfo(id = id, flags = expectedFlags))))
        },
        testM("ignores unknown flags") {
          val id    = 42L
          val input = respBulkString(s"flags=XYZ id=$id")
          for {
            res <- Task(ClientInfoOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk.single(ClientInfo(id = id, flags = Set.empty))))
        },
        testM("multiple") {
          val input = respBulkString(
            s"""sub=4 id=42 idle=6 fd=9234
               |id=99 events=r db=33
               |id=1 cmd=foo""".stripMargin
          )
          for {
            res <- Task(ClientInfoOutput.unsafeDecode(input))
          } yield assert(res)(
            equalTo(
              Chunk(
                ClientInfo(id = 42L, idle = Some(6.seconds), subscriptions = 4, fileDescriptor = Some(9234L)),
                ClientInfo(id = 99, events = ClientEvents(readable = true), databaseId = Some(33L)),
                ClientInfo(id = 1, lastCommand = Some("foo"))
              )
            )
          )
        }
      ),
      suite("ClientTrackingRedirect")(
        testM("Not enabled") {
          val input = respInt(-1L)
          for {
            res <- Task(ClientTrackingRedirectOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(ClientTrackingRedirect.NotEnabled))
        },
        testM("Not redirected") {
          val input = respInt(0L)
          for {
            res <- Task(ClientTrackingRedirectOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(ClientTrackingRedirect.NotRedirected))
        },
        testM("redirected") {
          val input = respInt(42L)
          for {
            res <- Task(ClientTrackingRedirectOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(ClientTrackingRedirect.RedirectedTo(input.value)))
        }
      )
    )

  private val Noise = "bad input"
}
