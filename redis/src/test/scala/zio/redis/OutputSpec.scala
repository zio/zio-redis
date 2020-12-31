package zio.redis

import zio.duration._
import zio.redis.Output._
import zio.redis.RedisError._
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Task }

object OutputSpec extends BaseSpec {

  def spec: Spec[Any, TestFailure[Throwable], TestSuccess] =
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
            res <-
              Task(ChunkOutput.unsafeDecode(RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))))
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        }
      ),
      suite("double")(
        testM("extract numbers") {
          val num = 42.3
          for {
            res <- Task(DoubleOutput.unsafeDecode(RespValue.bulkString(num.toString)))
          } yield assert(res)(equalTo(num))
        },
        testM("report number format exceptions as protocol errors") {
          val bad = "ok"
          for {
            res <- Task(DoubleOutput.unsafeDecode(RespValue.bulkString(bad))).either
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
            res <- Task(OptionalOutput(UnitOutput).unsafeDecode(RespValue.Null))
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
          val input = RespValue.array(
            RespValue.bulkString("5"),
            RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))
          )
          for {
            res <- Task(ScanOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(5L -> Chunk("foo", "bar")))
        }
      ),
      suite("string")(
        testM("extract strings") {
          for {
            res <- Task(MultiStringOutput.unsafeDecode(RespValue.bulkString(Noise)))
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
            res <- Task(KeyElemOutput.unsafeDecode(RespValue.Null))
          } yield assert(res)(isNone)
        },
        testM("extract key and element") {
          for {
            res <-
              Task(KeyElemOutput.unsafeDecode(RespValue.array(RespValue.bulkString("key"), RespValue.bulkString("a"))))
          } yield assert(res)(isSome(equalTo(("key", "a"))))
        },
        testM("report invalid input as protocol error") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(KeyElemOutput.unsafeDecode(input)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("multiStringChunk")(
        testM("extract one empty value") {
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(RespValue.Null))
          } yield assert(res)(isEmpty)
        },
        testM("extract one multi-string value") {
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(RespValue.bulkString("ab")))
          } yield assert(res)(hasSameElements(Chunk("ab")))
        },
        testM("extract one array value") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(MultiStringChunkOutput.unsafeDecode(input))
          } yield assert(res)(hasSameElements(Chunk("1", "2", "3")))
        }
      ),
      suite("chunkOptionalMultiString")(
        testM("extract one empty value") {
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(RespValue.Null))
          } yield assert(res)(isEmpty)
        },
        testM("extract array with one non-empty element") {
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(RespValue.array(RespValue.bulkString("ab"))))
          } yield assert(res)(equalTo(Chunk(Some("ab"))))
        },
        testM("extract array with multiple non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- Task(ChunkOptionalMultiStringOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), Some("2"), Some("3"))))
        },
        testM("extract array with empty and non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.Null, RespValue.bulkString("3"))
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
          val input = RespValue.array(
            RespValue.Integer(1L),
            RespValue.Null,
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- Task(ChunkOptionalLongOutput.unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), None, Some(2L), Some(3L))))
        },
        testM("extract array with non-empty elements") {
          val input = RespValue.array(
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
      suite("stream")(
        testM("extract valid input") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value"))
            )
          )

          Task(StreamOutput.unsafeDecode(input)).map(assert(_)(equalTo(Map("id" -> Map("field" -> "value")))))
        },
        testM("extract empty map") {
          Task(StreamOutput.unsafeDecode(RespValue.array())).map(assert(_)(isEmpty))
        },
        testM("error when array of field-value pairs has odd length") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value")),
              RespValue.Null
            )
          )

          Task(StreamOutput.unsafeDecode(input)).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message has more then two elements") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("a"), RespValue.bulkString("b"), RespValue.bulkString("c"))
            )
          )

          Task(StreamOutput.unsafeDecode(input)).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xPending")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when the smallest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.Null,
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, None, Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when the greatest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.Null,
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), None, Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        testM("extract when total number of pending messages is zero") {
          val input = RespValue.array(
            RespValue.Integer(0),
            RespValue.Null,
            RespValue.Null,
            RespValue.Null
          )
          Task(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(0L, None, None, Map.empty))))
        },
        testM("error when consumer array doesn't have two elements") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          Task(XPendingOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("pendingMessages")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100),
              RespValue.Integer(10)
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).map(
            assert(_)(
              hasSameElements(
                Chunk(
                  PendingMessage("id", "consumer", 100.millis, 10),
                  PendingMessage("id1", "consumer1", 101.millis, 11)
                )
              )
            )
          )
        },
        testM("error when message has more than four fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100),
              RespValue.Integer(10),
              RespValue.Null
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message has less than four fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100)
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          Task(PendingMessagesOutput.unsafeDecode(input)).either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xRead")(
        testM("extract valid value") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            ),
            RespValue.array(
              RespValue.bulkString("str2"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id2"),
                  RespValue.array(
                    RespValue.bulkString("c"),
                    RespValue.bulkString("d")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("id3"),
                  RespValue.array(
                    RespValue.bulkString("e"),
                    RespValue.bulkString("f")
                  )
                )
              )
            )
          )
          Task(XReadOutput.unsafeDecode(input)).map(
            assert(_)(
              equalTo(
                Map(
                  "str1" -> Map("id1" -> Map("a" -> "b")),
                  "str2" -> Map("id2" -> Map("c" -> "d"), "id3" -> Map("e" -> "f"))
                )
              )
            )
          )
        },
        testM("error when message content has odd number of fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a")
                  )
                )
              )
            )
          )
          Task(XReadOutput.unsafeDecode(input)).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when message doesn't have an ID") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("str1"),
              RespValue.array(
                RespValue.array(
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            )
          )
          Task(XReadOutput.unsafeDecode(input)).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        testM("error when stream doesn't have an ID") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("id1"),
                  RespValue.array(
                    RespValue.bulkString("a"),
                    RespValue.bulkString("b")
                  )
                )
              )
            )
          )
          Task(XReadOutput.unsafeDecode(input)).either.map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      )
    )

  private val Noise = "bad input"
}
