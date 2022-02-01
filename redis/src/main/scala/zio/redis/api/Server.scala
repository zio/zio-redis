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

  /**
   * The command shows a list of recent ACL security events:
   *   - Failures to authenticate their connections with AUTH or HELLO.
   *   - Commands denied because against the current ACL rules.
   *   - Commands denied because accessing keys not allowed in the current ACL rules.
   * @return
   *   list of log entries
   */
  final def aclLog(): ZIO[RedisExecutor, RedisError, Chunk[LogEntry]] = {
    val command = RedisCommand(AclLog, NoInput, ChunkOutput(LogEntryOutput))
    command.run(())
  }

  /**
   * The command shows a list of recent ACL security events:
   *   - Failures to authenticate their connections with AUTH or HELLO.
   *   - Commands denied because against the current ACL rules.
   *   - Commands denied because accessing keys not allowed in the current ACL rules.
   * @return
   *   OK if the security log was cleared.
   */
  final def aclLogReset(): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(AclLog, StringInput, UnitOutput)
    command.run("RESET")
  }

  /**
   * The command shows a list of recent ACL security events:
   *   - Failures to authenticate their connections with AUTH or HELLO.
   *   - Commands denied because against the current ACL rules.
   *   - Commands denied because accessing keys not allowed in the current ACL rules.
   * @return
   *   list of log entries
   */
  final def aclLog(count: Long): ZIO[RedisExecutor, RedisError, Chunk[LogEntry]] = {
    val command = RedisCommand(AclLog, LongInput, ChunkOutput(LogEntryOutput))
    command.run(count)
  }

  /**
   * When Redis is configured to use an ACL file (with the aclfile configuration option), this command will save the
   * currently defined ACLs from the server memory to the ACL file.
   * @return
   *   OK on success
   */
  final def aclSave(): ZIO[RedisExecutor, RedisError, Unit] = {
    val command = RedisCommand(AclLog, NoInput, UnitOutput)
    command.run(())
  }

  /**
   * The command shows a list of all the usernames of the currently configured users in the Redis ACL system.
   * @return
   *   an array of strings.
   */
  final def aclUsers(): ZIO[RedisExecutor, RedisError, Chunk[String]] = {
    val command = RedisCommand(AclUsers, NoInput, ChunkOutput(MultiStringOutput))
    command.run(())
  }

  /**
   * Return the username the current connection is authenticated with. New connections are authenticated with the
   * "default" user. They can change user using AUTH
   * @return
   *   he username of the current connection
   */
  final def aclWhoAmI(): ZIO[RedisExecutor, RedisError, String] = {
    val command = RedisCommand(AclWhoAmI, NoInput, MultiStringOutput)
    command.run(())
  }

  /**
   * Instruct Redis to start an Append Only File rewrite process. The rewrite will create a small optimized version of
   * the current Append Only File. If BGREWRITEAOF fails, no data gets lost as the old AOF will be untouched.
   * @return
   *   A simple string reply indicating that the rewriting started or is about to start ASAP, when the call is executed
   *   with success.
   */
  final def bgWriteAof(): ZIO[RedisExecutor, RedisError, String] = {
    val command = RedisCommand(BgWriteAof, NoInput, StringOutput)
    command.run(())
  }

  /**
   * Save the DB in background. Normally the OK code is immediately returned. Redis forks, the parent continues to serve
   * the clients, the child saves the DB on disk then exits.
   * @return
   *   Background saving started if BGSAVE started correctly or Background saving scheduled when used with the SCHEDULE
   *   subcommand.
   */
  final def bgSave(): ZIO[RedisExecutor, RedisError, String] = {
    val command = RedisCommand(BgSave, NoInput, StringOutput)
    command.run(())
  }

  /**
   * Save the DB in background. Normally the OK code is immediately returned. Redis forks, the parent continues to serve
   * the clients, the child saves the DB on disk then exits.
   * @return
   *   Background saving started if BGSAVE started correctly or Background saving scheduled when used with the SCHEDULE
   *   subcommand.
   */
  final def bgSaveSchedule(): ZIO[RedisExecutor, RedisError, String] = {
    val command = RedisCommand(BgSave, StringInput, StringOutput)
    command.run("SCHEDULE")
  }

  /**
   * Return an array with details about every Redis command. The COMMAND command is introspective. Its reply describes
   * all commands that the server can process. Redis clients can call it to obtain the server's runtime capabilities
   * during the handshake.
   * @return
   *   list of command details
   */
  final def command(): ZIO[RedisExecutor, RedisError, Chunk[CommandDetail]] = {
    val command = RedisCommand(Command, NoInput, ChunkOutput(CommandDetailOutput))
    command.run(())
  }

  /**
   * Returns Integer reply of number of total commands in this Redis server.
   * @return
   *  number of commands returned by COMMAND
   */
  final def commandCount(): ZIO[RedisExecutor, RedisError, Long] = {
    val command = RedisCommand(CommandCount, NoInput, LongOutput)
    command.run(())
  }
}

private[redis] object Server {
  final val AclCat       = "ACL CAT"
  final val AclDelUser   = "ACL DELUSER"
  final val AclGenPass   = "ACL GENPASS"
  final val AclSetUser   = "ACL SETUSER"
  final val AclGetUser   = "ACL GETUSER"
  final val AclList      = "ACL LIST"
  final val AclLoad      = "ACL LOAD"
  final val AclLog       = "ACL LOG"
  final val AclSave      = "ACL SAVE"
  final val AclUsers     = "ACL USERS"
  final val AclWhoAmI    = "ACL WHOAMI"
  final val BgWriteAof   = "BGREWRITEAOF"
  final val BgSave       = "BGSAVE"
  final val Command      = "COMMAND"
  final val CommandCount = "COMMAND COUNT"
}
