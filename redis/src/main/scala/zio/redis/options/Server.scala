package zio.redis.options

import zio.Chunk
import zio.duration._
import zio.prelude._

trait Server {

  sealed trait Rule { self =>
    private[redis] final def stringify: String =
      self match {
        case Rule.On                                    => "on"
        case Rule.Off                                   => "off"
        case Rule.KeyPattern(pattern)                   => s"~$pattern"
        case Rule.ResetKeys                             => "resetkeys"
        case Rule.ChannelPattern(pattern)               => s"&$pattern"
        case Rule.RestChannels                          => "resetchannels"
        case Rule.AddCommand(command, None)             => s"+$command"
        case Rule.AddCommand(command, Some(subcommand)) => s"+$command|$subcommand"
        case Rule.RemoveCommand(command)                => s"-$command"
        case Rule.AddCategory(category)                 => s"+@$category"
        case Rule.RemoveCategory(category)              => s"-@$category"
        case Rule.NoPass                                => "nopass"
        case Rule.AddTextPassword(password)             => s">$password"
        case Rule.AddHashedPassword(password)           => s"#$password"
        case Rule.RemoveTextPassword(password)          => s"<$password"
        case Rule.RemoveHashedPassword(password)        => s"!$password"
        case Rule.Reset                                 => "reset"
      }
  }

  object Rule {
    def unapply(value: String): Option[Rule] = value match {
      case "on"                     => Some(On)
      case "off"                    => Some(Off)
      case s"~$pattern"             => Some(KeyPattern(pattern))
      case "resetkeys"              => Some(ResetKeys)
      case s"&$pattern"             => Some(ChannelPattern(pattern))
      case s"+$command"             => Some(AddCommand(command))
      case s"+$command|$subcommand" => Some(AddCommand(command, Some(subcommand)))
      case s"-$command"             => Some(RemoveCommand(command))
      case s"+@$category"           => Some(AddCategory(category))
      case s"-@$category"           => Some(RemoveCategory(category))
      case "nopass"                 => Some(NoPass)
      case s">$password"            => Some(AddTextPassword(password))
      case s"#$password"            => Some(AddHashedPassword(password))
      case s"<$password"            => Some(RemoveTextPassword(password))
      case s"!$password"            => Some(RemoveHashedPassword(password))
      case "reset"                  => Some(Reset)
      case _                        => None
    }

    case object On                                                                   extends Rule
    case object Off                                                                  extends Rule
    sealed case class KeyPattern(pattern: String)                                    extends Rule
    case object ResetKeys                                                            extends Rule
    sealed case class ChannelPattern(pattern: String)                                extends Rule
    case object RestChannels                                                         extends Rule
    sealed case class AddCommand(command: String, subcommand: Option[String] = None) extends Rule
    sealed case class RemoveCommand(command: String)                                 extends Rule
    sealed case class AddCategory(category: String)                                  extends Rule
    sealed case class RemoveCategory(category: String)                               extends Rule
    case object NoPass                                                               extends Rule
    sealed case class AddTextPassword(pw: String)                                    extends Rule
    sealed case class RemoveTextPassword(pw: String)                                 extends Rule
    sealed case class AddHashedPassword(hash: String)                                extends Rule
    sealed case class RemoveHashedPassword(hash: String)                             extends Rule
    case object Reset                                                                extends Rule

    final val AllKeys: KeyPattern         = KeyPattern("*")
    final val AllChannels: ChannelPattern = ChannelPattern("*")
    final val AllCommands: AddCommand     = AddCommand("all")
    final val NoCommands: RemoveCommand   = RemoveCommand("all")
  }

  sealed case class UserInfo(
    flags: Chunk[String],
    passwords: Chunk[String],
    commands: String,
    keys: Chunk[String],
    channels: Chunk[String]
  )

  sealed case class UserEntry(username: String, rules: Chunk[Rule])

  object UserEntry {
    def unapply(value: String): Option[UserEntry] = value match {
      case s"user $username $rulesString" =>
        Chunk
          .fromArray(rulesString.split(' '))
          .forEach(Rule.unapply)
          .map(rules => UserEntry(username, rules))

      case _ =>
        None
    }
  }

  sealed case class LogEntry(
    count: Int,
    reason: String,
    context: String,
    `object`: String,
    username: String,
    ageDuration: Duration,
    clientInfo: String
  )

  sealed case class CommandDetail(
    name: String,
    artity: Int,
    flags: Chunk[String],
    firstKey: Int,
    lastKey: Int,
    step: Int,
    aclCategories: Chunk[String]
  )
}
