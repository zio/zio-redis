package zio.redis

import zio._
import zio.redis.Output._
import zio.redis.RedisError._
import zio.redis.internal.PubSub.{PushMessage, SubscriptionKey}
import zio.redis.internal.RespValue
import zio.redis.options.Cluster
import zio.test.Assertion._
import zio.test._

object OutputSpec extends BaseSpec {
  def spec: Spec[Any, Throwable] =
    suite("Output decoders")(
      suite("errors")(
        test("protocol errors") {
          for {
            res <- ZIO.attempt(BoolOutput.unsafeDecode(RespValue.Error(s"ERR $Noise"))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(Noise))))
        },
        test("wrong type") {
          for {
            res <- ZIO.attempt(BoolOutput.unsafeDecode(RespValue.Error(s"WRONGTYPE $Noise"))).either
          } yield assert(res)(isLeft(equalTo(WrongType(Noise))))
        }
      ),
      suite("boolean")(
        test("extract true") {
          for {
            res <- ZIO.attempt(BoolOutput.unsafeDecode(RespValue.Integer(1L)))
          } yield assert(res)(isTrue)
        },
        test("extract false") {
          for {
            res <- ZIO.attempt(BoolOutput.unsafeDecode(RespValue.Integer(0)))
          } yield assert(res)(isFalse)
        }
      ),
      suite("chunk")(
        test("extract empty arrays") {
          for {
            res <- ZIO.attempt(ChunkOutput(MultiStringOutput).unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        test("extract non-empty arrays") {
          val respValue = RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))
          for {
            res <- ZIO.attempt(ChunkOutput(MultiStringOutput).unsafeDecode(respValue))
          } yield assert(res)(hasSameElements(Chunk("foo", "bar")))
        }
      ),
      suite("double")(
        test("extract numbers") {
          val num = 42.3
          for {
            res <- ZIO.attempt(DoubleOutput.unsafeDecode(RespValue.bulkString(num.toString)))
          } yield assert(res)(equalTo(num))
        },
        test("extract numbers in scientific notation") {
          val num = 42.321e100d
          for {
            res <- ZIO.attempt(DoubleOutput.unsafeDecode(RespValue.bulkString(num.toString.toLowerCase)))
          } yield assert(res)(equalTo(num))
        },
        test("report number format exceptions as protocol errors") {
          val bad = "ok"
          for {
            res <- ZIO.attempt(DoubleOutput.unsafeDecode(RespValue.bulkString(bad))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"'$bad' isn't a double."))))
        }
      ),
      suite("double or infinity")(
        test("extract numbers") {
          val num = 42.3
          for {
            res <- ZIO.attempt(DoubleOrInfinity.unsafeDecode(RespValue.bulkString(num.toString)))
          } yield assert(res)(equalTo(num))
        },
        test("extract numbers in scientific notation") {
          val num = 42.321e100d
          for {
            res <- ZIO.attempt(DoubleOrInfinity.unsafeDecode(RespValue.bulkString(num.toString.toLowerCase)))
          } yield assert(res)(equalTo(num))
        },
        test("extract infinity") {
          for {
            res <- ZIO.attempt(DoubleOrInfinity.unsafeDecode(RespValue.bulkString("inf")))
          } yield assert(res)(equalTo(Double.PositiveInfinity))
        },
        test("extract negative infinity") {
          for {
            res <- ZIO.attempt(DoubleOrInfinity.unsafeDecode(RespValue.bulkString("-inf")))
          } yield assert(res)(equalTo(Double.NegativeInfinity))
        },
        test("report number format exceptions as protocol errors") {
          val bad = "ok"
          for {
            res <- ZIO.attempt(DoubleOrInfinity.unsafeDecode(RespValue.bulkString(bad))).either
          } yield assert(res)(isLeft(equalTo(ProtocolError(s"'$bad' isn't a double or an infinity."))))
        }
      ),
      suite("durations")(
        suite("milliseconds")(
          test("extract milliseconds") {
            val num = 42L
            for {
              res <- ZIO.attempt(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.millis))
          },
          test("report error for non-existing key") {
            for {
              res <- ZIO.attempt(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          test("report error for key without TTL") {
            for {
              res <- ZIO.attempt(DurationMillisecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        ),
        suite("seconds")(
          test("extract seconds") {
            val num = 42L
            for {
              res <- ZIO.attempt(DurationSecondsOutput.unsafeDecode(RespValue.Integer(num)))
            } yield assert(res)(equalTo(num.seconds))
          },
          test("report error for non-existing key") {
            for {
              res <- ZIO.attempt(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-2L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key not found."))))
          },
          test("report error for key without TTL") {
            for {
              res <- ZIO.attempt(DurationSecondsOutput.unsafeDecode(RespValue.Integer(-1L))).either
            } yield assert(res)(isLeft(equalTo(ProtocolError("Key has no expire."))))
          }
        )
      ),
      suite("long")(
        test("extract positive numbers") {
          val num = 42L
          for {
            res <- ZIO.attempt(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        },
        test("extract negative numbers") {
          val num = -42L
          for {
            res <- ZIO.attempt(LongOutput.unsafeDecode(RespValue.Integer(num)))
          } yield assert(res)(equalTo(num))
        }
      ),
      suite("chunkLong")(
        test("extract empty array") {
          for {
            res <- ZIO.attempt(ChunkOutput(LongOutput).unsafeDecode(RespValue.NullArray))
          } yield assert(res)(equalTo(Chunk.empty))
        },
        test("extract array of long values") {
          val respArray = RespValue.array(RespValue.Integer(1L), RespValue.Integer(2L), RespValue.Integer(3L))
          for {
            res <- ZIO.attempt(ChunkOutput(LongOutput).unsafeDecode(respArray))
          } yield assert(res)(equalTo(Chunk(1L, 2L, 3L)))
        },
        test("fail when one value is not a long") {
          val respArray =
            RespValue.array(RespValue.Integer(1L), RespValue.bulkString("not a long"), RespValue.Integer(3L))
          for {
            res <- ZIO.attempt(ChunkOutput(LongOutput).unsafeDecode(respArray)).exit
          } yield assert(res)(fails(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("optional")(
        test("extract null bulk string") {
          for {
            res <- ZIO.attempt(OptionalOutput(UnitOutput).unsafeDecode(RespValue.NullBulkString))
          } yield assert(res)(isNone)
        },
        test("extract null array") {
          for {
            res <- ZIO.attempt(OptionalOutput(ArbitraryOutput[String]()).unsafeDecode(RespValue.NullArray))
          } yield assert(res)(isNone)
        },
        test("extract some") {
          for {
            res <- ZIO.attempt(OptionalOutput(UnitOutput).unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isSome(isUnit))
        },
        test("extract empty bulk string") {
          for {
            res <-
              ZIO.attempt(OptionalOutput(ArbitraryOutput[String]()).unsafeDecode(RespValue.BulkString(Chunk.empty)))
          } yield assert(res)(isSome(equalTo("")))
        }
      ),
      suite("scan")(
        test("extract cursor and elements") {
          val input = RespValue.array(
            RespValue.bulkString("5"),
            RespValue.array(RespValue.bulkString("foo"), RespValue.bulkString("bar"))
          )
          for {
            res <- ZIO.attempt(ScanOutput(MultiStringOutput).unsafeDecode(input))
          } yield assert(res)(equalTo(5L -> Chunk("foo", "bar")))
        }
      ),
      suite("string")(
        test("extract strings") {
          for {
            res <- ZIO.attempt(MultiStringOutput.unsafeDecode(RespValue.bulkString(Noise)))
          } yield assert(res)(equalTo(Noise))
        }
      ),
      suite("unit")(
        test("extract unit") {
          for {
            res <- ZIO.attempt(UnitOutput.unsafeDecode(RespValue.SimpleString("OK")))
          } yield assert(res)(isUnit)
        }
      ),
      suite("reset") {
        test("extract unit") {
          for {
            res <- ZIO.attempt(ResetOutput.unsafeDecode(RespValue.SimpleString("RESET")))
          } yield assert(res)(isUnit)
        }
      },
      suite("keyElem")(
        test("extract none") {
          for {
            res <- ZIO.attempt(KeyElemOutput.unsafeDecode(RespValue.NullArray))
          } yield assert(res)(isNone)
        },
        test("extract key and element") {
          for {
            res <-
              ZIO.attempt(
                KeyElemOutput.unsafeDecode(RespValue.array(RespValue.bulkString("key"), RespValue.bulkString("a")))
              )
          } yield assert(res)(isSome(equalTo(("key", "a"))))
        },
        test("report invalid input as protocol error") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- ZIO.attempt(KeyElemOutput.unsafeDecode(input)).either
          } yield assert(res)(isLeft(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("multiStringChunk")(
        test("extract one empty value") {
          for {
            res <- ZIO.attempt(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(RespValue.NullBulkString))
          } yield assert(res)(isEmpty)
        },
        test("extract one multi-string value") {
          for {
            res <- ZIO.attempt(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(RespValue.bulkString("ab")))
          } yield assert(res)(hasSameElements(Chunk("ab")))
        },
        test("extract one array value") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- ZIO.attempt(MultiStringChunkOutput(MultiStringOutput).unsafeDecode(input))
          } yield assert(res)(hasSameElements(Chunk("1", "2", "3")))
        }
      ),
      suite("chunkOptionalMultiString")(
        test("extract one empty value") {
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(RespValue.NullArray))
          } yield assert(res)(isEmpty)
        },
        test("extract array with one non-empty element") {
          for {
            res <-
              ZIO.attempt(
                ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(RespValue.array(RespValue.bulkString("ab")))
              )
          } yield assert(res)(equalTo(Chunk(Some("ab"))))
        },
        test("extract array with multiple non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.bulkString("2"), RespValue.bulkString("3"))
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), Some("2"), Some("3"))))
        },
        test("extract array with empty and non-empty elements") {
          val input = RespValue.array(RespValue.bulkString("1"), RespValue.NullBulkString, RespValue.bulkString("3"))
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(MultiStringOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some("1"), None, Some("3"))))
        }
      ),
      suite("chunkOptionalLong")(
        test("extract one empty value") {
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(RespValue.Array(Chunk.empty)))
          } yield assert(res)(isEmpty)
        },
        test("extract array with empty and non-empty elements") {
          val input = RespValue.array(
            RespValue.Integer(1L),
            RespValue.NullBulkString,
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), None, Some(2L), Some(3L))))
        },
        test("extract array with non-empty elements") {
          val input = RespValue.array(
            RespValue.Integer(1L),
            RespValue.Integer(1L),
            RespValue.Integer(2L),
            RespValue.Integer(3L)
          )
          for {
            res <- ZIO.attempt(ChunkOutput(OptionalOutput(LongOutput)).unsafeDecode(input))
          } yield assert(res)(equalTo(Chunk(Some(1L), Some(1L), Some(2L), Some(3L))))
        }
      ),
      suite("stream")(
        test("extract valid input") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value"))
            )
          )

          ZIO
            .attempt(StreamEntriesOutput[String, String, String]().unsafeDecode(input))
            .map(assert(_)(equalTo(Chunk(StreamEntry("id", Map("field" -> "value"))))))
        },
        test("extract empty map") {
          ZIO
            .attempt(StreamEntriesOutput[String, String, String]().unsafeDecode(RespValue.array()))
            .map(assert(_)(isEmpty))
        },
        test("error when array of field-value pairs has odd length") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("field"), RespValue.bulkString("value")),
              RespValue.NullBulkString
            )
          )

          ZIO
            .attempt(StreamEntriesOutput[String, String, String]().unsafeDecode(input))
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        test("error when message has more then two elements") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.array(RespValue.bulkString("a"), RespValue.bulkString("b"), RespValue.bulkString("c"))
            )
          )

          ZIO
            .attempt(StreamEntriesOutput[String, String, String]().unsafeDecode(input))
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xPending")(
        test("extract valid value") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          ZIO
            .attempt(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        test("extract when the smallest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.NullBulkString,
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          ZIO
            .attempt(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, None, Some("b"), Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        test("extract when the greatest ID is null") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.NullBulkString,
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1"), RespValue.bulkString("1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          ZIO
            .attempt(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(1L, Some("a"), None, Map("consumer1" -> 1L, "consumer2" -> 2L)))))
        },
        test("extract when total number of pending messages is zero") {
          val input = RespValue.array(
            RespValue.Integer(0),
            RespValue.NullBulkString,
            RespValue.NullBulkString,
            RespValue.NullArray
          )
          ZIO
            .attempt(XPendingOutput.unsafeDecode(input))
            .map(assert(_)(equalTo(PendingInfo(0L, None, None, Map.empty))))
        },
        test("error when consumer array doesn't have two elements") {
          val input = RespValue.array(
            RespValue.Integer(1),
            RespValue.bulkString("a"),
            RespValue.bulkString("b"),
            RespValue.array(
              RespValue.array(RespValue.bulkString("consumer1")),
              RespValue.array(RespValue.bulkString("consumer2"), RespValue.bulkString("2"))
            )
          )
          ZIO
            .attempt(XPendingOutput.unsafeDecode(input))
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("pendingMessages")(
        test("extract valid value") {
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
          ZIO
            .attempt(PendingMessagesOutput.unsafeDecode(input))
            .map(
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
        test("error when message has more than four fields") {
          val input = RespValue.array(
            RespValue.array(
              RespValue.bulkString("id"),
              RespValue.bulkString("consumer"),
              RespValue.Integer(100),
              RespValue.Integer(10),
              RespValue.NullBulkString
            ),
            RespValue.array(
              RespValue.bulkString("id1"),
              RespValue.bulkString("consumer1"),
              RespValue.Integer(101),
              RespValue.Integer(11)
            )
          )
          ZIO
            .attempt(PendingMessagesOutput.unsafeDecode(input))
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        test("error when message has less than four fields") {
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
          ZIO
            .attempt(PendingMessagesOutput.unsafeDecode(input))
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        }
      ),
      suite("xRead")(
        test("extract valid value") {
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
          ZIO
            .attempt(
              ChunkOutput(StreamOutput[String, String, String, String]()).unsafeDecode(input)
            )
            .map(
              assert(_)(
                equalTo(
                  Chunk(
                    StreamChunk("str1", Chunk(StreamEntry("id1", Map("a" -> "b")))),
                    StreamChunk("str2", Chunk(StreamEntry("id2", Map("c" -> "d")), StreamEntry("id3", Map("e" -> "f"))))
                  )
                )
              )
            )
        },
        test("error when message content has odd number of fields") {
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
          ZIO
            .attempt(
              KeyValueOutput(ArbitraryOutput[String](), StreamEntriesOutput[String, String, String]())
                .unsafeDecode(input)
            )
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        test("error when message doesn't have an ID") {
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
          ZIO
            .attempt(
              KeyValueOutput(ArbitraryOutput[String](), StreamEntriesOutput[String, String, String]())
                .unsafeDecode(input)
            )
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        test("error when stream doesn't have an ID") {
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
          ZIO
            .attempt(
              KeyValueOutput(ArbitraryOutput[String](), StreamEntriesOutput[String, String, String]())
                .unsafeDecode(input)
            )
            .either
            .map(assert(_)(isLeft(isSubtype[ProtocolError](anything))))
        },
        suite("xInfoStream")(
          test("extract valid value with first and last entry") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("2-0"),
              RespValue.bulkString("max-deleted-entry-id"),
              RespValue.bulkString("3-0"),
              RespValue.bulkString("entries-added"),
              RespValue.Integer(5),
              RespValue.bulkString("recorded-first-entry-id"),
              RespValue.bulkString("1-0"),
              RespValue.bulkString("groups"),
              RespValue.Integer(2),
              RespValue.bulkString("first-entry"),
              RespValue.array(
                RespValue.bulkString("1-0"),
                RespValue.array(
                  RespValue.bulkString("key1"),
                  RespValue.bulkString("value1")
                )
              ),
              RespValue.bulkString("last-entry"),
              RespValue.array(
                RespValue.bulkString("2-0"),
                RespValue.array(
                  RespValue.bulkString("key2"),
                  RespValue.bulkString("value2")
                )
              )
            )

            assertZIO(ZIO.attempt(StreamInfoOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfo(
                  1,
                  2,
                  3,
                  "2-0",
                  "3-0",
                  5,
                  "1-0",
                  2,
                  Some(StreamEntry("1-0", Map("key1" -> "value1"))),
                  Some(StreamEntry("2-0", Map("key2" -> "value2")))
                )
              )
            )
          },
          test("extract valid value without first and last entry") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("2-0"),
              RespValue.bulkString("max-deleted-entry-id"),
              RespValue.bulkString("3-0"),
              RespValue.bulkString("entries-added"),
              RespValue.Integer(5),
              RespValue.bulkString("recorded-first-entry-id"),
              RespValue.bulkString("1-0"),
              RespValue.bulkString("groups"),
              RespValue.Integer(2)
            )

            assertZIO(ZIO.attempt(StreamInfoOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(StreamInfo[String, String, String](1, 2, 3, "2-0", "3-0", 5, "1-0", 2, None, None))
            )
          }
        ),
        suite("xInfoGroups")(
          test("extract a valid value") {
            val resp = RespValue.array(
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("group1"),
                RespValue.bulkString("consumers"),
                RespValue.Integer(1),
                RespValue.bulkString("pending"),
                RespValue.Integer(1),
                RespValue.bulkString("last-delivered-id"),
                RespValue.bulkString("1-0"),
                RespValue.bulkString("entries-read"),
                RespValue.Integer(1),
                RespValue.bulkString("lag"),
                RespValue.Integer(1)
              ),
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("group2"),
                RespValue.bulkString("consumers"),
                RespValue.Integer(2),
                RespValue.bulkString("pending"),
                RespValue.Integer(2),
                RespValue.bulkString("last-delivered-id"),
                RespValue.bulkString("2-0"),
                RespValue.bulkString("entries-read"),
                RespValue.Integer(2),
                RespValue.bulkString("lag"),
                RespValue.Integer(2)
              )
            )

            assertZIO(ZIO.attempt(StreamGroupsInfoOutput.unsafeDecode(resp)))(
              equalTo(
                Chunk(
                  StreamGroupsInfo("group1", 1, 1, "1-0", 1, 1),
                  StreamGroupsInfo("group2", 2, 2, "2-0", 2, 2)
                )
              )
            )
          },
          test("extract an empty array") {
            val resp = RespValue.array()

            assertZIO(ZIO.attempt(StreamGroupsInfoOutput.unsafeDecode(resp)))(isEmpty)
          }
        ),
        suite("xInfoConsumers")(
          test("extract a valid value") {
            val resp = RespValue.array(
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("consumer1"),
                RespValue.bulkString("pending"),
                RespValue.Integer(1),
                RespValue.bulkString("idle"),
                RespValue.Integer(100),
                RespValue.bulkString("inactive"),
                RespValue.Integer(100)
              ),
              RespValue.array(
                RespValue.bulkString("name"),
                RespValue.bulkString("consumer2"),
                RespValue.bulkString("pending"),
                RespValue.Integer(2),
                RespValue.bulkString("idle"),
                RespValue.Integer(200),
                RespValue.bulkString("inactive"),
                RespValue.Integer(200)
              )
            )

            assertZIO(ZIO.attempt(StreamConsumersInfoOutput.unsafeDecode(resp)))(
              equalTo(
                Chunk(
                  StreamConsumersInfo("consumer1", 1, 100.millis, 100.millis),
                  StreamConsumersInfo("consumer2", 2, 200.millis, 200.millis)
                )
              )
            )
          },
          test("extract an empty array") {
            val resp = RespValue.array()

            assertZIO(ZIO.attempt(StreamConsumersInfoOutput.unsafeDecode(resp)))(isEmpty)
          }
        ),
        suite("xInfoStreamFull")(
          test("extract a valid value without groups") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0"),
              RespValue.bulkString("max-deleted-entry-id"),
              RespValue.bulkString("1-0"),
              RespValue.bulkString("entries-added"),
              RespValue.Integer(4),
              RespValue.bulkString("recorded-first-entry-id"),
              RespValue.bulkString("2-0"),
              RespValue.bulkString("entries"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("3-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("4-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                )
              )
            )

            assertZIO(ZIO.attempt(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo(
                  1,
                  2,
                  3,
                  "0-0",
                  "1-0",
                  4,
                  "2-0",
                  Chunk(StreamEntry("3-0", Map("key1" -> "value1")), StreamEntry("4-0", Map("key1" -> "value1"))),
                  Chunk.empty
                )
              )
            )
          },
          test("extract a valid value without groups and entries") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0"),
              RespValue.bulkString("max-deleted-entry-id"),
              RespValue.bulkString("1-0"),
              RespValue.bulkString("entries-added"),
              RespValue.Integer(4),
              RespValue.bulkString("recorded-first-entry-id"),
              RespValue.bulkString("2-0")
            )
            assertZIO(ZIO.attempt(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo[String, String, String](1, 2, 3, "0-0", "1-0", 4, "2-0", Chunk.empty, Chunk.empty)
              )
            )
          },
          test("extract an empty array") {
            val resp = RespValue.array()
            assertZIO(ZIO.attempt(StreamConsumersInfoOutput.unsafeDecode(resp)))(isEmpty)
          },
          test("extract a valid value with groups and entries") {
            val resp = RespValue.array(
              RespValue.bulkString("length"),
              RespValue.Integer(1),
              RespValue.bulkString("radix-tree-keys"),
              RespValue.Integer(2),
              RespValue.bulkString("radix-tree-nodes"),
              RespValue.Integer(3),
              RespValue.bulkString("last-generated-id"),
              RespValue.bulkString("0-0"),
              RespValue.bulkString("max-deleted-entry-id"),
              RespValue.bulkString("1-0"),
              RespValue.bulkString("entries-added"),
              RespValue.Integer(4),
              RespValue.bulkString("recorded-first-entry-id"),
              RespValue.bulkString("2-0"),
              RespValue.bulkString("entries"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("3-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                ),
                RespValue.array(
                  RespValue.bulkString("4-0"),
                  RespValue.array(
                    RespValue.bulkString("key1"),
                    RespValue.bulkString("value1")
                  )
                )
              ),
              RespValue.bulkString("groups"),
              RespValue.array(
                RespValue.array(
                  RespValue.bulkString("name"),
                  RespValue.bulkString("name1"),
                  RespValue.bulkString("last-delivered-id"),
                  RespValue.bulkString("lastDeliveredId"),
                  RespValue.bulkString("entries-read"),
                  RespValue.Integer(1),
                  RespValue.bulkString("lag"),
                  RespValue.Integer(2),
                  RespValue.bulkString("pel-count"),
                  RespValue.Integer(3),
                  RespValue.bulkString("pending"),
                  RespValue.array(
                    RespValue.array(
                      RespValue.bulkString("entryId1"),
                      RespValue.bulkString("consumerName1"),
                      RespValue.Integer(1588152520299L),
                      RespValue.Integer(1)
                    ),
                    RespValue.array(
                      RespValue.bulkString("entryId2"),
                      RespValue.bulkString("consumerName2"),
                      RespValue.Integer(1588152520299L),
                      RespValue.Integer(1)
                    )
                  ),
                  RespValue.bulkString("consumers"),
                  RespValue.array(
                    RespValue.array(
                      RespValue.bulkString("name"),
                      RespValue.bulkString("Alice"),
                      RespValue.bulkString("seen-time"),
                      RespValue.Integer(1588152520299L),
                      RespValue.bulkString("active-time"),
                      RespValue.Integer(1588152520299L),
                      RespValue.bulkString("pel-count"),
                      RespValue.Integer(1),
                      RespValue.bulkString("pending"),
                      RespValue.array(
                        RespValue.array(
                          RespValue.bulkString("entryId3"),
                          RespValue.Integer(1588152520299L),
                          RespValue.Integer(1)
                        )
                      )
                    )
                  )
                )
              )
            )

            assertZIO(ZIO.attempt(StreamInfoFullOutput[String, String, String]().unsafeDecode(resp)))(
              equalTo(
                StreamInfoWithFull.FullStreamInfo(
                  1,
                  2,
                  3,
                  "0-0",
                  "1-0",
                  4,
                  "2-0",
                  Chunk(StreamEntry("3-0", Map("key1" -> "value1")), StreamEntry("4-0", Map("key1" -> "value1"))),
                  Chunk(
                    StreamInfoWithFull.ConsumerGroups(
                      "name1",
                      "lastDeliveredId",
                      1,
                      2,
                      3,
                      Chunk(
                        StreamInfoWithFull.GroupPel("entryId1", "consumerName1", 1588152520299L.millis, 1L),
                        StreamInfoWithFull.GroupPel("entryId2", "consumerName2", 1588152520299L.millis, 1L)
                      ),
                      Chunk(
                        StreamInfoWithFull.Consumers(
                          "Alice",
                          1588152520299L.millis,
                          1588152520299L.millis,
                          1,
                          Chunk(
                            StreamInfoWithFull.ConsumerPel("entryId3", 1588152520299L.millis, 1)
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          }
        )
      ),
      suite("PushMessage")(
        test("subscribe") {
          val channel   = "foo"
          val numOfSubs = 1L

          val input =
            RespValue.array(
              RespValue.bulkString("subscribe"),
              RespValue.bulkString(channel),
              RespValue.Integer(numOfSubs)
            )

          val expected = PushMessage.Subscribed(SubscriptionKey.Channel(channel), numOfSubs)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        },
        test("psubscribe") {
          val pattern   = "f*"
          val numOfSubs = 1L

          val input =
            RespValue.array(
              RespValue.bulkString("psubscribe"),
              RespValue.bulkString(pattern),
              RespValue.Integer(numOfSubs)
            )

          val expected = PushMessage.Subscribed(SubscriptionKey.Pattern(pattern), numOfSubs)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        },
        test("unsubscribe") {
          val channel   = "foo"
          val numOfSubs = 1L

          val input =
            RespValue.array(
              RespValue.bulkString("unsubscribe"),
              RespValue.bulkString(channel),
              RespValue.Integer(numOfSubs)
            )

          val expected = PushMessage.Unsubscribed(SubscriptionKey.Channel(channel), numOfSubs)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        },
        test("punsubscribe") {
          val pattern   = "f*"
          val numOfSubs = 1L

          val input =
            RespValue.array(
              RespValue.bulkString("punsubscribe"),
              RespValue.bulkString(pattern),
              RespValue.Integer(numOfSubs)
            )

          val expected = PushMessage.Unsubscribed(SubscriptionKey.Pattern(pattern), numOfSubs)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        },
        test("message") {
          val channel = "foo"
          val message = RespValue.bulkString("bar")

          val input =
            RespValue.array(
              RespValue.bulkString("message"),
              RespValue.bulkString(channel),
              message
            )

          val expected = PushMessage.Message(SubscriptionKey.Channel(channel), channel, message)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        },
        test("pmessage") {
          val pattern = "f*"
          val channel = "foo"
          val message = RespValue.bulkString("bar")

          val input =
            RespValue.array(
              RespValue.bulkString("pmessage"),
              RespValue.bulkString(pattern),
              RespValue.bulkString(channel),
              message
            )

          val expected = PushMessage.Message(SubscriptionKey.Pattern(pattern), channel, message)

          assertZIO(ZIO.attempt(PushMessageOutput.unsafeDecode(input)))(equalTo(expected))
        }
      ),
      suite("ClusterPartition")(
        test("3 masters cluster") {
          val response =
            RespValue.array(
              RespValue.array(
                RespValue.Integer(0L),
                RespValue.Integer(5460L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.1"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node1"),
                  RespValue.array()
                )
              ),
              RespValue.array(
                RespValue.Integer(5461L),
                RespValue.Integer(10922L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.2"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node2"),
                  RespValue.array()
                )
              ),
              RespValue.array(
                RespValue.Integer(10923L),
                RespValue.Integer(16383L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.3"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node3"),
                  RespValue.array()
                )
              )
            )

          val expected = Chunk(
            Cluster.Partition(
              Cluster.SlotRange(0L, 5460L),
              Cluster.Node("node1", RedisUri("127.0.0.1", 6379)),
              slaves = Chunk.empty
            ),
            Cluster.Partition(
              Cluster.SlotRange(5461L, 10922L),
              Cluster.Node("node2", RedisUri("127.0.0.2", 6379)),
              slaves = Chunk.empty
            ),
            Cluster.Partition(
              Cluster.SlotRange(10923L, 16383L),
              Cluster.Node("node3", RedisUri("127.0.0.3", 6379)),
              slaves = Chunk.empty
            )
          )

          assertZIO(ZIO.attempt(ChunkOutput(ClusterPartitionOutput).unsafeDecode(response)))(hasSameElements(expected))
        },
        test("3 masters with 2 replicas cluster") {
          val response =
            RespValue.array(
              RespValue.array(
                RespValue.Integer(0L),
                RespValue.Integer(5460L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.1"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node1"),
                  RespValue.array()
                ),
                RespValue.array(
                  RespValue.bulkString("127.0.1.1"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("replica1"),
                  RespValue.array()
                )
              ),
              RespValue.array(
                RespValue.Integer(5461L),
                RespValue.Integer(10922L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.2"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node2"),
                  RespValue.array()
                ),
                RespValue.array(
                  RespValue.bulkString("127.0.1.2"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("replica2"),
                  RespValue.array()
                )
              ),
              RespValue.array(
                RespValue.Integer(10923L),
                RespValue.Integer(16383L),
                RespValue.array(
                  RespValue.bulkString("127.0.0.3"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("node3"),
                  RespValue.array()
                ),
                RespValue.array(
                  RespValue.bulkString("127.0.1.3"),
                  RespValue.Integer(6379L),
                  RespValue.bulkString("replica3"),
                  RespValue.array()
                )
              )
            )

          val expected = Chunk(
            Cluster.Partition(
              Cluster.SlotRange(0L, 5460L),
              Cluster.Node("node1", RedisUri("127.0.0.1", 6379)),
              slaves = Chunk.single(Cluster.Node("replica1", RedisUri("127.0.1.1", 6379)))
            ),
            Cluster.Partition(
              Cluster.SlotRange(5461L, 10922L),
              Cluster.Node("node2", RedisUri("127.0.0.2", 6379)),
              slaves = Chunk.single(Cluster.Node("replica2", RedisUri("127.0.1.2", 6379)))
            ),
            Cluster.Partition(
              Cluster.SlotRange(10923L, 16383L),
              Cluster.Node("node3", RedisUri("127.0.0.3", 6379)),
              slaves = Chunk.single(Cluster.Node("replica3", RedisUri("127.0.1.3", 6379)))
            )
          )

          assertZIO(ZIO.attempt(ChunkOutput(ClusterPartitionOutput).unsafeDecode(response)))(hasSameElements(expected))
        }
      )
    )

  private val Noise = "bad input"
}
