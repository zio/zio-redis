package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis.{RedisCommand, RedisError, RedisExecutor}
import zio.{Chunk, ZIO}

trait Server {
  import Server._

  /**
   * The command shows the available ACL categories if called without arguments. If a category name is given, the
   * command shows all the Redis commands in the specified category.
   *
   * @param categoryName
   *  the category name
   * @return
   *  a list of ACL categories or a list of commands inside a given category.
   */
  final def aclCat(categoryName: Option[String]): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(AclCat, OptionalInput(StringInput), ChunkOutput(MultiStringOutput))
    command.run(categoryName)
  }

  /**
   * Create an ACL user with the specified rules or modify the rules of an existing user.
   *
   * @param username
   *  the username
   * @param rules
   *  rules to apply for the given user
   * @return
   *  the Unit value
   */
  final def aclSetUser(username: String, rules: String*): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(AclSetUser, Tuple2(StringInput, Varargs(StringInput)), UnitOutput)
    command.run((username, rules))
  }

  /**
   * Delete all the specified ACL users and terminate all the connections that are authenticated with such users.
   *
   * @param username
   *  the username
   * @param usernames
   *  the rest of the usernames
   * @return
   *  The number of users that were deleted.
   */
  final def aclDelUser(username: String, usernames: String*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(AclDelUser, NonEmptyList(StringInput), LongOutput)
    command.run((username, usernames.toList))
  }

  /**
   * The ACL GENPASS command generates a password starting from /dev/urandom if available, otherwise
   * (in systems without /dev/urandom) it uses a weaker system that is likely still better than picking a weak
   * password by hand.
   *
   * @param bits
   *  the output string length is the number of specified bits (rounded to the next multiple of 4) divided by 4
   * @return
   *  the generated password
   */
  final def aclGenPass(bits: Option[Long]): ZIO[RedisExecutor, RedisError, Chunk[Byte]] = {
    val command = RedisCommand(AclGenPass, OptionalInput(LongInput), BulkStringOutput)
    command.run(bits)
  }

  final def aclGetUser(username: String): ZIO[RedisExecutor, RedisError, ]
}

private[redis] object Server {
  final val AclCat = "ACL CAT"
  final val AclDelUser = "ACL DELUSER"
  final val AclGenPass = "ACL GENPASS"
  final val AclSetUser = "ACL SETUSER"
  final val AclGetUser = "ACL GETUSER"
}