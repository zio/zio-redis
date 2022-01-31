package zio.redis
import zio.Chunk
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test._
import zio.duration._
import zio.test.TestAspect.eventually

trait ServerSpec extends BaseSpec {

  val serverSpec =
    suite("server")(
      suite("acl cat")(
        testM("acl cat with no category parameter") {
          for {
            aclList <- aclCat()
          } yield assert(aclList)(isNonEmpty)
        },
        testM("acl cat with some category parameter") {
          for {
            aclList <- aclCat("dangerous")
          } yield assert(aclList)(isNonEmpty)
        },
        testM("acl cat with another category parameter") {
          for {
            aclList <- aclCat("hyperloglog")
          } yield assert(aclList)(isNonEmpty)
        }
      ),
      suite("acl setuser")(
        testM("acl setuser without rules") {
          for {
            user   <- uuid
            result <- aclSetUser(user)
          } yield assert(result)(isUnit)
        },
        testM("acl setuser with rules") {
          for {
            user   <- uuid
            result <- aclSetUser(user, Rule.AllKeys, Rule.AddCategory("string"), Rule.AddCategory("set"))
          } yield assert(result)(isUnit)
        },
        testM("acl setuser with invalid rules") {
          for {
            user   <- uuid
            result <- aclSetUser(user, Rule.AddCategory("heeyyyy")).run
          } yield assert(result)(fails(isSubtype[ProtocolError](anything)))
        }
      ),
      suite("acl deluser")(
        testM("acl deluser singel user") {
          for {
            user   <- uuid
            _      <- aclSetUser(user)
            result <- aclDelUser(user)
          } yield assert(result)(equalTo(1L))
        },
        testM("acl deluser multiple users") {
          for {
            user1  <- uuid
            user2  <- uuid
            _      <- aclSetUser(user1)
            _      <- aclSetUser(user2)
            result <- aclDelUser(user1, user2)
          } yield assert(result)(equalTo(2L))
        },
        testM("acl deluser non existing user") {
          for {
            user   <- uuid
            result <- aclDelUser(user)
          } yield assert(result)(equalTo(0L))
        }
      ),
      suite("acl genpass")(
        testM("acl genpass") {
          for {
            pw <- aclGenPass(None)
          } yield assert(pw)(isNonEmpty)
        },
        testM("acl genpass with bits parameter") {
          for {
            pw <- aclGenPass(Some(32L))
          } yield assert(pw)(isNonEmpty)
        }
      ),
      suite("acl getuser")(
        testM("get default user") {
          for {
            userInfo <- aclGetUser("default")
          } yield assert(userInfo)(isSubtype[UserInfo](anything))
        }
      ),
      suite("acl list")(
        testM("list users") {
          for {
            users <- aclList()
          } yield assert(users)(isSubtype[Chunk[UserEntry]](anything))
        }
      ),
      suite("acl load")(
        testM("load acl and succeed or fail") {
          for {
            result <- aclLoad().either
          } yield {
            assert(result)(isLeft) || assert(result)(isRight(isUnit))
          }
        }
      ),
      suite("acl log")(
        testM("acl log without parameters") {
          for {
            _ <- auth("Someuser", "wrongpassword").either
            log <- aclLog()
          } yield {
            assert(log)(isSubtype[Chunk[Chunk[String]]](anything))
          }
        },
        testM("acl log with reset") {
          for {
            _ <- auth("Someuser", "wrongpassword").either
            res <- aclLogReset()
          } yield {
            assert(res)(isUnit)
          }
        },
        testM("acl log with count") {
          for {
            _ <- auth("Someuser", "wrongpassword").either
            res <- aclLog(1L)
          } yield {
            assert(res)(isSubtype[Chunk[Chunk[String]]](anything))
          }
        },
        testM("acl log empty after reset") {
          for {
            _ <- auth("Someuser", "wrongpassword").either
            _ <- aclLogReset()
            res <- aclLog()
          } yield {
            assert(res)(isEmpty)
          }
        }
      ),
      suite("acl save")(
        testM("error or success (depends on the redis server config") {
          for {
            res <- aclSave().either
          } yield {
            assert(res)(isLeft) || assert(res)(isRight)
          }
        }
      ),
      suite("acl users")(
        testM("successfully list all usernames") {
          for {
            res <- aclUsers()
          } yield assert(res)(isSubtype[Chunk[String]](anything))
        }
      ),
      suite("acl whoami")(
        testM("successfully get my username") {
          for {
            res <- aclWhoAmI()
          } yield assert(res)(isSubtype[String](anything))
        }
      ),
      suite("acl bgwriteaof")(
        testM("successfully call bgwriteaof") {
          for {
            res <- bgWriteAof()
          } yield assert(res)(isSubtype[String](anything))
        }
      ),
      suite("acl bgsave")(
        testM("successfully call bgsave") {
          for {
            _ <- zio.clock.sleep(1.second)
            res <- bgSave()
          } yield assert(res)(isSubtype[String](anything))
        } @@ eventually,
        testM("successfully call bgsave") {
          for {
            _ <- zio.clock.sleep(1.second)
            res <- bgSaveSchedule()
          } yield assert(res)(isSubtype[String](anything))
        } @@ eventually
      )

    )

}
