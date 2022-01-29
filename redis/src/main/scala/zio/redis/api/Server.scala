package zio.redis.api

import zio.redis.Input._
import zio.redis.Output._
import zio.redis._
import zio.{Chunk, ZIO}

trait Server {
  import Server._

  /**
   * The command shows the available ACL categories if called without arguments. If a category name is given, the
   * command shows all the Redis commands in the specified category.
   *
   * @param categoryName
   *   the category name
   * @return
   *   a list of server commands for a given category
   */
  final def aclCat(categoryName: String): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(AclCat, StringInput, ChunkOutput(MultiStringOutput))
    command.run(categoryName)
  }

  /**
   * The command shows the available ACL categories if called without arguments. If a category name is given, the
   * command shows all the Redis commands in the specified category.
   *
   * @return
   *   a list of categories
   */
  final def aclCat(): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(AclCat, NoInput, ChunkOutput(MultiStringOutput))
    command.run(())
  }

  /**
   * Create an ACL user with the specified rules or modify the rules of an existing user.
   *
   * @param username
   *   the username
   * @param rules
   *   rules to apply for the given user
   * @return
   *   the Unit value
   */
  final def aclSetUser(username: String, rules: Rule*): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(AclSetUser, Tuple2(StringInput, Varargs(AclRuleInput)), UnitOutput)
    command.run((username, rules))
  }

  /**
   * Delete all the specified ACL users and terminate all the connections that are authenticated with such users.
   *
   * @param username
   *   the username
   * @param usernames
   *   the rest of the usernames
   * @return
   *   The number of users that were deleted.
   */
  final def aclDelUser(username: String, usernames: String*): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(AclDelUser, NonEmptyList(StringInput), LongOutput)
    command.run((username, usernames.toList))
  }

  /**
   * The ACL GENPASS command generates a password starting from /dev/urandom if available, otherwise (in systems without
   * /dev/urandom) it uses a weaker system that is likely still better than picking a weak password by hand.
   *
   * @param bits
   *   the output string length is the number of specified bits (rounded to the next multiple of 4) divided by 4
   * @return
   *   the generated password
   */
  final def aclGenPass(bits: Option[Long]): ZIO[RedisExecutor, RedisError, Chunk[Byte]] = {
    val command = RedisCommand(AclGenPass, OptionalInput(LongInput), BulkStringOutput)
    command.run(bits)
  }

  /**
   * The command returns all the rules defined for an existing ACL user. Specifically, it lists the user's ACL flags,
   * password hashes and key name patterns.
   *
   * @param username
   *   the username to request
   * @return
   *   the users information
   */
  final def aclGetUser(username: String): ZIO[RedisExecutor, RedisError, UserInfo] = {
    val command = RedisCommand(AclGetUser, StringInput, UserinfoOutput)
    command.run(username)
  }

  /**
   * The command shows the currently active ACL rules in the Redis server. Each line in the returned array defines a
   * different user, and the format is the same used in the redis.conf file or the external ACL file, so you can cut and
   * paste what is returned by the ACL LIST command directly inside a configuration file if you wish
   * @return
   *   the user entries of the server
   */
  final def aclList(): ZIO[RedisExecutor, RedisError, Chunk[UserEntry]] = {
    val command = RedisCommand(AclList, NoInput, ChunkOutput(UserEntryOutput))
    command.run(())
  }

  /**
   * When Redis is configured to use an ACL file (with the aclfile configuration option), this command will reload the
   * ACLs from the file, replacing all the current ACL rules with the ones defined in the file.
   * @return
   *   load acl file was successful
   */
  final def aclLoad(): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(AclLoad, NoInput, UnitOutput)
    command.run(())
  }
}

private[redis] object Server {
  final val AclCat     = "ACL CAT"
  final val AclDelUser = "ACL DELUSER"
  final val AclGenPass = "ACL GENPASS"
  final val AclSetUser = "ACL SETUSER"
  final val AclGetUser = "ACL GETUSER"
  final val AclList    = "ACL LIST"
  final val AclLoad    = "ACL LOAD"
}
