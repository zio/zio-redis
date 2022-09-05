package zio.redis

import zio._
import zio.redis.Input._
import zio.test.Assertion._
import zio.test._

import java.net.InetAddress
import java.time.Instant

object InputSpec extends BaseSpec {
  import BitFieldCommand._
  import BitFieldType._
  import BitOperation._
  import Order._
  import RadiusUnit._
  import StrAlgoLcsQueryType._

  def spec: Spec[TestConfig, Throwable] =
    suite("Input encoders")(
      suite("AbsTtl")(
        test("valid value") {
          for {
            result <- ZIO.attempt(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(respArgs("ABSTTL")))
        }
      ),
      suite("Address")(
        test("valid value") {
          for {
            ip     <- ZIO.succeed(InetAddress.getByName("127.0.0.1"))
            port   <- ZIO.succeed(42)
            result <- ZIO.attempt(AddressInput.encode(Address(ip, port)))
          } yield assert(result)(equalTo(respArgs("127.0.0.1:42")))
        }
      ),
      suite("Aggregate")(
        test("max") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MAX")))
        },
        test("min") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "MIN")))
        },
        test("sum") {
          for {
            result <- ZIO.attempt(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(respArgs("AGGREGATE", "SUM")))
        }
      ),
      suite("Alpha")(
        test("alpha") {
          for {
            result <- ZIO.attempt(AlphaInput.encode(Alpha))
          } yield assert(result)(equalTo(respArgs("ALPHA")))
        }
      ),
      suite("Auth")(
        test("with empty password") {
          for {
            result <- ZIO.attempt(AuthInput.encode(Auth("")))
          } yield assert(result)(equalTo(respArgs("AUTH", "")))
        },
        test("with non-empty password") {
          for {
            result <- ZIO.attempt(AuthInput.encode(Auth("pass")))
          } yield assert(result)(equalTo(respArgs("AUTH", "pass")))
        }
      ),
      suite("Bool")(
        test("true") {
          for {
            result <- ZIO.attempt(BoolInput.encode(true))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        test("false") {
          for {
            result <- ZIO.attempt(BoolInput.encode(false))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("Stralgocommand")(
        test("length option") {
          assert(StralgoLcsQueryTypeInput.encode(StrAlgoLcsQueryType.Len))(
            equalTo(respArgs("LEN"))
          )
        },
        test("idx option default") {
          assert(StralgoLcsQueryTypeInput.encode(Idx()))(
            equalTo(respArgs("IDX"))
          )
        },
        test("idx option with minmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(minMatchLength = 2)))(
            equalTo(respArgs("IDX", "MINMATCHLEN", "2"))
          )
        },
        test("idx option with withmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(withMatchLength = true)))(
            equalTo(respArgs("IDX", "WITHMATCHLEN"))
          )
        },
        test("idx option with minmatchlength and withmatchlength") {
          assert(StralgoLcsQueryTypeInput.encode(Idx(minMatchLength = 2, withMatchLength = true)))(
            equalTo(respArgs("IDX", "MINMATCHLEN", "2", "WITHMATCHLEN"))
          )
        }
      ),
      suite("BitFieldCommand")(
        test("get with unsigned type and positive offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "2")))
        },
        test("get with signed type and negative offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(respArgs("GET", "i3", "-2")))
        },
        test("get with unsigned type and zero offset") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(respArgs("GET", "u3", "0")))
        },
        test("set with unsigned type, positive offset and positive value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "2", "100")))
        },
        test("set with signed type, negative offset and negative value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("SET", "i3", "-2", "-100")))
        },
        test("set with unsigned type, zero offset and zero value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("SET", "u3", "0", "0")))
        },
        test("incr with unsigned type, positive offset and positive value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "2", "100")))
        },
        test("incr with signed type, negative offset and negative value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "i3", "-2", "-100")))
        },
        test("incr with unsigned type, zero offset and zero value") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(respArgs("INCRBY", "u3", "0", "0")))
        },
        test("overflow sat") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "SAT")))
        },
        test("overflow fail") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "FAIL")))
        },
        test("overflow warp") {
          for {
            result <- ZIO.attempt(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(respArgs("OVERFLOW", "WRAP")))
        }
      ),
      suite("BitOperation")(
        test("and") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(respArgs("AND")))
        },
        test("or") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(respArgs("OR")))
        },
        test("xor") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(respArgs("XOR")))
        },
        test("not") {
          for {
            result <- ZIO.attempt(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(respArgs("NOT")))
        }
      ),
      suite("BitPosRange")(
        test("with only start") {
          for {
            result <- ZIO.attempt(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        test("with start and the end") {
          for {
            result <- ZIO.attempt(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(respArgs("0", "1000")))
        }
      ),
      suite("By")(
        test("with a pattern") {
          for {
            result <- ZIO.attempt(ByInput.encode("mykey_*"))
          } yield assert(result)(equalTo(respArgs("BY", "mykey_*")))
        }
      ),
      suite("Changed")(
        test("valid value") {
          for {
            result <- ZIO.attempt(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(respArgs("CH")))
        }
      ),
      suite("ClientKill")(
        test("address") {
          for {
            address <- ZIO.succeed(InetAddress.getByName("127.0.0.1"))
            port    <- ZIO.succeed(42)
            result  <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.Address(address, port)))
          } yield assert(result)(equalTo(respArgs("ADDR", "127.0.0.1:42")))
        },
        test("local address") {
          for {
            address <- ZIO.succeed(InetAddress.getByName("127.0.0.1"))
            port    <- ZIO.succeed(42)
            result  <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.LocalAddress(address, port)))
          } yield assert(result)(equalTo(respArgs("LADDR", s"127.0.0.1:42")))
        },
        test("client id") {
          for {
            id     <- ZIO.succeed(42L)
            result <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.Id(id)))
          } yield assert(result)(equalTo(respArgs("ID", "42")))
        },
        test("type") {
          for {
            clientType <- ZIO.succeed(ClientType.PubSub)
            result     <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.Type(clientType)))
          } yield assert(result)(equalTo(respArgs("TYPE", "pubsub")))
        },
        test("user") {
          for {
            user   <- ZIO.succeed("Foo Bar")
            result <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.User(user)))
          } yield assert(result)(equalTo(respArgs("USER", "Foo Bar")))
        },
        test("skip me") {
          for {
            result <- ZIO.attempt(ClientKillInput.encode(ClientKillFilter.SkipMe(true)))
          } yield assert(result)(equalTo(respArgs("SKIPME", "YES")))
        }
      ),
      suite("ClientList")(
        test("filter by id") {
          for {
            filter <- ZIO.succeed(ClientListFilter.Id(zio.prelude.NonEmptyList(12)))
            result <- ZIO.attempt(ClientListInput.encode(filter))
          } yield assert(result)(equalTo(respArgs("ID", "12")))
        },
        test("filter by more than one id") {
          for {
            filter <- ZIO.succeed(ClientListFilter.Id(zio.prelude.NonEmptyList(12, 13, 14)))
            result <- ZIO.attempt(ClientListInput.encode(filter))
          } yield assert(result)(equalTo(respArgs("ID", "12", "13", "14")))
        },
        test("filter by type")(check(genClientListType) { clientType =>
          for {
            filter <- ZIO.succeed(ClientListFilter.Type(clientType))
            result <- ZIO.attempt(ClientListInput.encode(filter))
          } yield assert(result)(equalTo(respArgs("TYPE", clientType.toString.toUpperCase)))
        })
      ),
      suite("ClientPauseMode")(
        test("all") {
          for {
            result <- ZIO.attempt(ClientPauseModeInput.encode(ClientPauseMode.All))
          } yield assert(result)(equalTo(respArgs("ALL")))
        },
        test("write") {
          for {
            result <- ZIO.attempt(ClientPauseModeInput.encode(ClientPauseMode.Write))
          } yield assert(result)(equalTo(respArgs("WRITE")))
        }
      ),
      suite("ClientTracking")(
        test("off") {
          for {
            result <- ZIO.attempt(ClientTrackingInput.encode(None))
          } yield assert(result)(equalTo(respArgs("OFF")))
        },
        test("client redirect with noloop and prefixes") {
          for {
            clientId <- ZIO.succeed(42L)
            prefixes <- ZIO.succeed(Chunk("prefix1", "prefix2", "prefix3"))
            result   <- ZIO.attempt(ClientTrackingInput.encode(Some((Some(clientId), None, true, prefixes))))
          } yield assert(result)(
            equalTo(
              respArgs("ON", "REDIRECT", clientId.toString) ++ prefixes
                .flatMap(respArgs("PREFIX", _)) ++ respArgs("NOLOOP")
            )
          )
        },
        test("broadcast mode") {
          for {
            result <-
              ZIO.attempt(
                ClientTrackingInput.encode(Some((None, Some(ClientTrackingMode.Broadcast), false, Chunk.empty)))
              )
          } yield assert(result)(equalTo(respArgs("ON", "BCAST")))
        }
      ),
      suite("Copy")(
        test("valid value") {
          for {
            result <- ZIO.attempt(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(respArgs("COPY")))
        }
      ),
      suite("Count")(
        test("positive value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "3")))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(-3L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "-3")))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(CountInput.encode(Count(0L)))
          } yield assert(result)(equalTo(respArgs("COUNT", "0")))
        }
      ),
      suite("Position")(
        test("before") {
          for {
            result <- ZIO.attempt(PositionInput.encode(Position.Before))
          } yield assert(result)(equalTo(respArgs("BEFORE")))
        },
        test("after") {
          for {
            result <- ZIO.attempt(PositionInput.encode(Position.After))
          } yield assert(result)(equalTo(respArgs("AFTER")))
        }
      ),
      suite("RedisType")(
        test("string type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.String))
          } yield assert(result)(equalTo(respArgs("TYPE", "string")))
        },
        test("list type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.List))
          } yield assert(result)(equalTo(respArgs("TYPE", "list")))
        },
        test("set type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Set))
          } yield assert(result)(equalTo(respArgs("TYPE", "set")))
        },
        test("sorted set type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.SortedSet))
          } yield assert(result)(equalTo(respArgs("TYPE", "zset")))
        },
        test("hash type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Hash))
          } yield assert(result)(equalTo(respArgs("TYPE", "hash")))
        },
        test("stream type") {
          for {
            result <- ZIO.attempt(RedisTypeInput.encode(RedisType.Stream))
          } yield assert(result)(equalTo(respArgs("TYPE", "stream")))
        }
      ),
      suite("Double")(
        test("positive value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(4.2d))
          } yield assert(result)(equalTo(respArgs("4.2")))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(-4.2d))
          } yield assert(result)(equalTo(respArgs("-4.2")))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(DoubleInput.encode(0d))
          } yield assert(result)(equalTo(respArgs("0.0")))
        }
      ),
      suite("DurationMilliseconds")(
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationMillisecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1000")))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationMillisecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("100")))
        }
      ),
      suite("DurationSeconds")(
        test("1 minute") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(1.minute))
          } yield assert(result)(equalTo(respArgs("60")))
        },
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("1")))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationSecondsInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("DurationTtl")(
        test("1 second") {
          for {
            result <- ZIO.attempt(DurationTtlInput.encode(1.second))
          } yield assert(result)(equalTo(respArgs("PX", "1000")))
        },
        test("100 milliseconds") {
          for {
            result <- ZIO.attempt(DurationTtlInput.encode(100.millis))
          } yield assert(result)(equalTo(respArgs("PX", "100")))
        }
      ),
      suite("Freq")(
        test("empty string") {
          for {
            result <- ZIO.attempt(FreqInput.encode(Freq("")))
          } yield assert(result)(equalTo(respArgs("FREQ", "")))
        },
        test("non-empty string") {
          for {
            result <- ZIO.attempt(FreqInput.encode(Freq("frequency")))
          } yield assert(result)(equalTo(respArgs("FREQ", "frequency")))
        }
      ),
      suite("Get")(
        test("with a pattern") {
          for {
            result <- ZIO.attempt(GetInput.encode("mypattern_*"))
          } yield assert(result)(equalTo(respArgs("GET", "mypattern_*")))
        }
      ),
      suite("IdleTime")(
        test("0 seconds") {
          for {
            result <- ZIO.attempt(IdleTimeInput.encode(IdleTime(0)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "0")))
        },
        test("5 seconds") {
          for {
            result <- ZIO.attempt(IdleTimeInput.encode(IdleTime(5)))
          } yield assert(result)(equalTo(respArgs("IDLETIME", "5")))
        }
      ),
      suite("Increment")(
        test("valid value") {
          for {
            result <- ZIO.attempt(IncrementInput.encode(Increment))
          } yield assert(result)(equalTo(respArgs("INCR")))
        }
      ),
      suite("KeepTtl")(
        test("valid value") {
          for {
            result <- ZIO.attempt(KeepTtlInput.encode(KeepTtl))
          } yield assert(result)(equalTo(respArgs("KEEPTTL")))
        }
      ),
      suite("LexRange")(
        test("with unbound min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "+")))
        },
        test("with open min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "+")))
        },
        test("with closed min and unbound max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Unbounded.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "+")))
        },
        test("with unbound min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "(z")))
        },
        test("with open min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "(z")))
        },
        test("with closed min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Open("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "(z")))
        },
        test("with unbound min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Unbounded.stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-", "[z")))
        },
        test("with open min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Open("a").stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(a", "[z")))
        },
        test("with closed min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((LexMinimum.Closed("a").stringify, LexMaximum.Closed("z").stringify))
                      )
          } yield assert(result)(equalTo(respArgs("[a", "[z")))
        }
      ),
      suite("Limit")(
        test("with positive offset and positive count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(4L, 5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "4", "5")))
        },
        test("with negative offset and negative count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(-4L, -5L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "-4", "-5")))
        },
        test("with zero offset and zero count") {
          for {
            result <- ZIO.attempt(LimitInput.encode(Limit(0L, 0L)))
          } yield assert(result)(equalTo(respArgs("LIMIT", "0", "0")))
        }
      ),
      suite("Long")(
        test("positive value") {
          for {
            result <- ZIO.attempt(LongInput.encode(4L))
          } yield assert(result)(equalTo(respArgs("4")))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(LongInput.encode(-4L))
          } yield assert(result)(equalTo(respArgs("-4")))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(LongInput.encode(0L))
          } yield assert(result)(equalTo(respArgs("0")))
        }
      ),
      suite("LongLat")(
        test("positive longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(4.2d, 5.2d)))
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        },
        test("negative longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(-4.2d, -5.2d)))
          } yield assert(result)(equalTo(respArgs("-4.2", "-5.2")))
        },
        test("zero longitude and latitude") {
          for {
            result <- ZIO.attempt(LongLatInput.encode(LongLat(0d, 0d)))
          } yield assert(result)(equalTo(respArgs("0.0", "0.0")))
        }
      ),
      suite("MemberScore")(
        test("with positive score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(4.2d, "")))
          } yield assert(result)(equalTo(respArgs("4.2", "")))
        },
        test("with negative score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(-4.2d, "")))
          } yield assert(result)(equalTo(respArgs("-4.2", "")))
        },
        test("with zero score and empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(0d, "")))
          } yield assert(result)(equalTo(respArgs("0.0", "")))
        },
        test("with positive score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("4.2", "member")))
        },
        test("with negative score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(-4.2d, "member")))
          } yield assert(result)(equalTo(respArgs("-4.2", "member")))
        },
        test("with zero score and non-empty member") {
          for {
            result <- ZIO.attempt(MemberScoreInput[String]().encode(MemberScore(0d, "member")))
          } yield assert(result)(equalTo(respArgs("0.0", "member")))
        }
      ),
      suite("NoInput")(
        test("valid value") {
          for {
            result <- ZIO.attempt(NoInput.encode(()))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("NonEmptyList")(
        test("with multiple elements") {
          for {
            result <- ZIO.attempt(NonEmptyList(StringInput).encode(("a", List("b", "c"))))
          } yield assert(result)(equalTo(respArgs("a", "b", "c")))
        },
        test("with one element") {
          for {
            result <- ZIO.attempt(NonEmptyList(StringInput).encode(("a", List.empty)))
          } yield assert(result)(equalTo(respArgs("a")))
        }
      ),
      suite("Order")(
        test("ascending") {
          for {
            result <- ZIO.attempt(OrderInput.encode(Ascending))
          } yield assert(result)(equalTo(respArgs("ASC")))
        },
        test("descending") {
          for {
            result <- ZIO.attempt(OrderInput.encode(Descending))
          } yield assert(result)(equalTo(respArgs("DESC")))
        }
      ),
      suite("RadiusUnit")(
        test("meters") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Meters))
          } yield assert(result)(equalTo(respArgs("m")))
        },
        test("kilometers") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Kilometers))
          } yield assert(result)(equalTo(respArgs("km")))
        },
        test("feet") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Feet))
          } yield assert(result)(equalTo(respArgs("ft")))
        },
        test("miles") {
          for {
            result <- ZIO.attempt(RadiusUnitInput.encode(Miles))
          } yield assert(result)(equalTo(respArgs("mi")))
        }
      ),
      suite("Range")(
        test("with positive start and positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(Range(1, 5)))
          } yield assert(result)(equalTo(respArgs("1", "5")))
        },
        test("with negative start and positive end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(Range(-1, 5)))
          } yield assert(result)(equalTo(respArgs("-1", "5")))
        },
        test("with positive start and negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(Range(1, -5)))
          } yield assert(result)(equalTo(respArgs("1", "-5")))
        },
        test("with negative start and negative end") {
          for {
            result <- ZIO.attempt(RangeInput.encode(Range(-1, -5)))
          } yield assert(result)(equalTo(respArgs("-1", "-5")))
        }
      ),
      suite("Pattern")(
        test("with valid pattern") {
          for {
            result <- ZIO.attempt(PatternInput.encode(Pattern("*[ab]-*")))
          } yield assert(result)(equalTo(respArgs("MATCH", "*[ab]-*")))
        },
        test("with empty pattern") {
          for {
            result <- ZIO.attempt(PatternInput.encode(Pattern("")))
          } yield assert(result)(equalTo(respArgs("MATCH", "")))
        }
      ),
      suite("Replace")(
        test("valid value") {
          for {
            result <- ZIO.attempt(ReplaceInput.encode(Replace))
          } yield assert(result)(equalTo(respArgs("REPLACE")))
        }
      ),
      suite("StoreDist")(
        test("with non-empty string") {
          for {
            result <- ZIO.attempt(StoreDistInput.encode(StoreDist("key")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "key")))
        },
        test("with empty string") {
          for {
            result <- ZIO.attempt(StoreDistInput.encode(StoreDist("")))
          } yield assert(result)(equalTo(respArgs("STOREDIST", "")))
        }
      ),
      suite("Store")(
        test("with non-empty string") {
          for {
            result <- ZIO.attempt(StoreInput.encode(Store("key")))
          } yield assert(result)(equalTo(respArgs("STORE", "key")))
        },
        test("with empty string") {
          for {
            result <- ZIO.attempt(StoreInput.encode(Store("")))
          } yield assert(result)(equalTo(respArgs("STORE", "")))
        }
      ),
      suite("ScoreRange")(
        test("with infinite min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "+inf")))
        },
        test("with open min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "+inf")))
        },
        test("with closed min and infinite max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Infinity.stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "+inf")))
        },
        test("with infinite min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "(5.2")))
        },
        test("with open min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "(5.2")))
        },
        test("with closed min and open max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Open(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "(5.2")))
        },
        test("with infinite min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Infinity.stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("-inf", "5.2")))
        },
        test("with open min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Open(4.2d).stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("(4.2", "5.2")))
        },
        test("with closed min and closed max") {
          for {
            result <- ZIO.attempt(
                        Tuple2(ArbitraryInput[String](), ArbitraryInput[String]())
                          .encode((ScoreMinimum.Closed(4.2d).stringify, ScoreMaximum.Closed(5.2d).stringify))
                      )
          } yield assert(result)(equalTo(respArgs("4.2", "5.2")))
        }
      ),
      suite("String")(
        test("non-empty value") {
          for {
            result <- ZIO.attempt(StringInput.encode("non-empty"))
          } yield assert(result)(equalTo(respArgs("non-empty")))
        },
        test("empty value") {
          for {
            result <- ZIO.attempt(StringInput.encode(""))
          } yield assert(result)(equalTo(respArgs("")))
        }
      ),
      suite("Optional")(
        test("none") {
          for {
            result <- ZIO.attempt(OptionalInput(LongInput).encode(None))
          } yield assert(result)(isEmpty)
        },
        test("some") {
          for {
            result <- ZIO.attempt(OptionalInput(LongInput).encode(Some(2L)))
          } yield assert(result)(equalTo(respArgs("2")))
        }
      ),
      suite("TimeSeconds")(
        test("positiv value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3")))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(TimeSecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3")))
        }
      ),
      suite("TimeMilliseconds")(
        test("positiv value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(3L)))
          } yield assert(result)(equalTo(respArgs("3000")))
        },
        test("zero value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(0L)))
          } yield assert(result)(equalTo(respArgs("0")))
        },
        test("negative value") {
          for {
            result <- ZIO.attempt(TimeMillisecondsInput.encode(Instant.ofEpochSecond(-3L)))
          } yield assert(result)(equalTo(respArgs("-3000")))
        }
      ),
      suite("Tuple2")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple2(StringInput, LongInput).encode(("one", 2L)))
          } yield assert(result)(equalTo(respArgs("one", "2")))
        }
      ),
      suite("Tuple3")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple3(StringInput, LongInput, StringInput).encode(("one", 2, "three")))
          } yield assert(result)(equalTo(respArgs("one", "2", "three")))
        }
      ),
      suite("Tuple4")(
        test("valid value") {
          for {
            result <- ZIO.attempt(Tuple4(StringInput, LongInput, StringInput, LongInput).encode(("one", 2, "three", 4)))
          } yield assert(result)(equalTo(respArgs("one", "2", "three", "4")))
        }
      ),
      suite("Tuple5")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple5(StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five"))
                      )
          } yield assert(result)(
            equalTo(respArgs("one", "2", "three", "4", "five"))
          )
        }
      ),
      suite("Tuple7")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple7(StringInput, LongInput, StringInput, LongInput, StringInput, LongInput, StringInput)
                          .encode(("one", 2, "three", 4, "five", 6, "seven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven"
              )
            )
          )
        }
      ),
      suite("Tuple9")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple9(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine"
              )
            )
          )
        }
      ),
      suite("Tuple11")(
        test("valid value") {
          for {
            result <- ZIO.attempt(
                        Tuple11(
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput,
                          LongInput,
                          StringInput
                        ).encode(("one", 2, "three", 4, "five", 6, "seven", 8, "nine", 10, "eleven"))
                      )
          } yield assert(result)(
            equalTo(
              respArgs(
                "one",
                "2",
                "three",
                "4",
                "five",
                "6",
                "seven",
                "8",
                "nine",
                "10",
                "eleven"
              )
            )
          )
        }
      ),
      suite("Update")(
        test("set existing") {
          for {
            result <- ZIO.attempt(UpdateInput.encode(Update.SetExisting))
          } yield assert(result)(equalTo(respArgs("XX")))
        },
        test("set new") {
          for {
            result <- ZIO.attempt(UpdateInput.encode(Update.SetNew))
          } yield assert(result)(equalTo(respArgs("NX")))
        }
      ),
      suite("Id")(
        test("valid value") {
          for {
            result <- ZIO.attempt(IdInput.encode(10))
          } yield assert(result)(equalTo(respArgs("ID", "10")))
        }
      ),
      suite("UnblockBehavior")(
        test("timeout") {
          for {
            result <- ZIO.attempt(UnblockBehaviorInput.encode(UnblockBehavior.Timeout))
          } yield assert(result)(equalTo(respArgs("TIMEOUT")))
        },
        test("error") {
          for {
            result <- ZIO.attempt(UnblockBehaviorInput.encode(UnblockBehavior.Error))
          } yield assert(result)(equalTo(respArgs("ERROR")))
        }
      ),
      suite("Varargs")(
        test("with multiple elements") {
          for {
            result <- ZIO.attempt(Varargs(LongInput).encode(List(1, 2, 3)))
          } yield assert(result)(equalTo(respArgs("1", "2", "3")))
        },
        test("with no elements") {
          for {
            result <- ZIO.attempt(Varargs(LongInput).encode(List.empty))
          } yield assert(result)(isEmpty)
        }
      ),
      suite("WithScores")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithScoresInput.encode(WithScores))
          } yield assert(result)(equalTo(respArgs("WITHSCORES")))
        }
      ),
      suite("WithCoord")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithCoordInput.encode(WithCoord))
          } yield assert(result)(equalTo(respArgs("WITHCOORD")))
        }
      ),
      suite("WithDist")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithDistInput.encode(WithDist))
          } yield assert(result)(equalTo(respArgs("WITHDIST")))
        }
      ),
      suite("WithHash")(
        test("valid value") {
          for {
            result <- ZIO.attempt(WithHashInput.encode(WithHash))
          } yield assert(result)(equalTo(respArgs("WITHHASH")))
        }
      ),
      suite("Idle")(
        test("with 1 second") {
          ZIO
            .attempt(IdleInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("IDLE", "1000"))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(IdleInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("IDLE", "100"))))
        },
        test("with negative duration") {
          ZIO
            .attempt(IdleInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("IDLE", "-1000"))))
        }
      ),
      suite("Time")(
        test("with 1 second") {
          ZIO
            .attempt(TimeInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("TIME", "1000"))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(TimeInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("TIME", "100"))))
        },
        test("with negative duration") {
          ZIO
            .attempt(TimeInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("TIME", "-1000"))))
        }
      ),
      suite("RetryCount")(
        test("with positive count") {
          ZIO
            .attempt(RetryCountInput.encode(100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "100"))))
        },
        test("with negative count") {
          ZIO
            .attempt(RetryCountInput.encode(-100))
            .map(assert(_)(equalTo(respArgs("RETRYCOUNT", "-100"))))
        }
      ),
      suite("XGroupCreate")(
        test("without mkStream") {
          ZIO
            .attempt(
              XGroupCreateInput[String, String, String]().encode(
                XGroupCommand.Create("key", "group", "id", mkStream = false)
              )
            )
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id"))))
        },
        test("with mkStream") {
          ZIO
            .attempt(
              XGroupCreateInput[String, String, String]().encode(
                XGroupCommand.Create("key", "group", "id", mkStream = true)
              )
            )
            .map(assert(_)(equalTo(respArgs("CREATE", "key", "group", "id", "MKSTREAM"))))
        }
      ),
      suite("XGroupSetId")(
        test("valid value") {
          ZIO
            .attempt(XGroupSetIdInput[String, String, String]().encode(XGroupCommand.SetId("key", "group", "id")))
            .map(assert(_)(equalTo(respArgs("SETID", "key", "group", "id"))))
        }
      ),
      suite("XGroupDestroy")(
        test("valid value") {
          ZIO
            .attempt(XGroupDestroyInput[String, String]().encode(XGroupCommand.Destroy("key", "group")))
            .map(assert(_)(equalTo(respArgs("DESTROY", "key", "group"))))
        }
      ),
      suite("XGroupCreateConsumer")(
        test("valid value") {
          ZIO
            .attempt(
              XGroupCreateConsumerInput[String, String, String]().encode(
                XGroupCommand.CreateConsumer("key", "group", "consumer")
              )
            )
            .map(assert(_)(equalTo(respArgs("CREATECONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("XGroupDelConsumer")(
        test("valid value") {
          ZIO
            .attempt(
              XGroupDelConsumerInput[String, String, String]().encode(
                XGroupCommand.DelConsumer("key", "group", "consumer")
              )
            )
            .map(assert(_)(equalTo(respArgs("DELCONSUMER", "key", "group", "consumer"))))
        }
      ),
      suite("Block")(
        test("with 1 second") {
          ZIO
            .attempt(BlockInput.encode(1.second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "1000"))))
        },
        test("with 100 milliseconds") {
          ZIO
            .attempt(BlockInput.encode(100.millis))
            .map(assert(_)(equalTo(respArgs("BLOCK", "100"))))
        },
        test("with negative duration") {
          ZIO
            .attempt(BlockInput.encode((-1).second))
            .map(assert(_)(equalTo(respArgs("BLOCK", "-1000"))))
        }
      ),
      suite("Streams")(
        test("with one pair") {
          ZIO
            .attempt(StreamsInput[String, String]().encode(("a" -> "b", Chunk.empty)))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "b"))))
        },
        test("with multiple pairs") {
          ZIO
            .attempt(StreamsInput[String, String]().encode(("a" -> "b", Chunk.single("c" -> "d"))))
            .map(assert(_)(equalTo(respArgs("STREAMS", "a", "c", "b", "d"))))
        }
      ),
      suite("NoAck")(
        test("valid value") {
          ZIO.attempt(NoAckInput.encode(NoAck)).map(assert(_)(equalTo(respArgs("NOACK"))))
        }
      ),
      suite("MaxLen")(
        test("with approximate") {
          ZIO
            .attempt(StreamMaxLenInput.encode(StreamMaxLen(approximate = true, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "~", "10"))))
        },
        test("without approximate") {
          ZIO
            .attempt(StreamMaxLenInput.encode(StreamMaxLen(approximate = false, 10)))
            .map(assert(_)(equalTo(respArgs("MAXLEN", "10"))))
        }
      ),
      suite("WithForce")(
        test("valid value") {
          ZIO.attempt(WithForceInput.encode(WithForce)).map(assert(_)(equalTo(respArgs("FORCE"))))
        }
      ),
      suite("WithJustId")(
        test("valid value") {
          ZIO.attempt(WithJustIdInput.encode(WithJustId)).map(assert(_)(equalTo(respArgs("JUSTID"))))
        }
      ),
      suite("Side")(
        test("left") {
          for {
            result <- ZIO.attempt(SideInput.encode(Side.Left))
          } yield assert(result)(equalTo(respArgs("LEFT")))
        },
        test("right") {
          for {
            result <- ZIO.attempt(SideInput.encode(Side.Right))
          } yield assert(result)(equalTo(respArgs("RIGHT")))
        }
      ),
      suite("ListMaxLen")(
        test("valid value") {
          ZIO.attempt(ListMaxLenInput.encode(ListMaxLen(10L))).map(assert(_)(equalTo(respArgs("MAXLEN", "10"))))
        }
      ),
      suite("Rank")(
        test("valid value") {
          ZIO.attempt(RankInput.encode(Rank(10L))).map(assert(_)(equalTo(respArgs("RANK", "10"))))
        }
      ),
      suite("GetEx")(
        test("GetExInput - valid value") {
          for {
            resultSeconds <-
              ZIO.attempt(GetExInput[String]().encode(scala.Tuple3.apply("key", Expire.SetExpireSeconds, 1.second)))
            resultMilliseconds <-
              ZIO.attempt(GetExInput[String]().encode(scala.Tuple3("key", Expire.SetExpireMilliseconds, 100.millis)))
          } yield assert(resultSeconds)(equalTo(respArgs("key", "EX", "1"))) && assert(resultMilliseconds)(
            equalTo(respArgs("key", "PX", "100"))
          )
        },
        test("GetExAtInput - valid value") {
          for {
            resultSeconds <-
              ZIO.attempt(
                GetExAtInput[String]().encode(
                  scala.Tuple3("key", ExpiredAt.SetExpireAtSeconds, Instant.parse("2021-04-06T00:00:00Z"))
                )
              )
            resultMilliseconds <-
              ZIO.attempt(
                GetExAtInput[String]().encode(
                  scala.Tuple3("key", ExpiredAt.SetExpireAtMilliseconds, Instant.parse("2021-04-06T00:00:00Z"))
                )
              )
          } yield assert(resultSeconds)(equalTo(respArgs("key", "EXAT", "1617667200"))) && assert(resultMilliseconds)(
            equalTo(respArgs("key", "PXAT", "1617667200000"))
          )
        },
        test("GetExPersistInput - valid value") {
          for {
            result              <- ZIO.attempt(GetExPersistInput[String]().encode("key" -> true))
            resultWithoutOption <- ZIO.attempt(GetExPersistInput[String]().encode("key" -> false))
          } yield assert(result)(equalTo(respArgs("key", "PERSIST"))) &&
            assert(resultWithoutOption)(equalTo(respArgs("key")))
        }
      ),
      suite("YesNo")(
        test("yes") {
          for {
            result <- ZIO.attempt(YesNoInput.encode(true))
          } yield assert(result)(equalTo(respArgs("YES")))
        },
        test("no") {
          for {
            result <- ZIO.attempt(YesNoInput.encode(false))
          } yield assert(result)(equalTo(respArgs("NO")))
        }
      )
    )

  private def respArgs(xs: String*): Chunk[RespValue] = Chunk.fromIterable(xs.map(RespValue.bulkString))
}
