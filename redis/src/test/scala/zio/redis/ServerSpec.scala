package zio.redis
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test._

trait ServerSpec extends BaseSpec {

  val serverSpec =
    suite("server")(
      suite("acl cat")(
        testM("acl cat with no category parameter") {
          for {
            aclList <- aclCat(None)
          } yield assert(aclList)(isNonEmpty)
        },
        testM("acl cat with some category parameter") {
          for {
            aclList <- aclCat(Some("dangerous"))
          } yield assert(aclList)(isNonEmpty)
        },
        testM("acl cat with some category unknown parameter") {
          for {
            aclList <- aclCat(Some("foobar")).run
          } yield assert(aclList)(fails(isSubtype[ProtocolError](anything)))
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
            result <- aclSetUser(user, "allkeys", "+@string", "+@set")
          } yield assert(result)(isUnit)
        },
        testM("acl setuser with invalid rules") {
          for {
            user   <- uuid
            result <- aclSetUser(user, "heeyyyy").run
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
      suite("acl genpass") (
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
      )
    )

}