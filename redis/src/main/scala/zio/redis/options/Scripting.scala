package zio.redis.options

trait Scripting {
  sealed trait DebugMode { self =>
    private[redis] final def stringify: String =
      self match {
        case DebugMode.Yes  => "YES"
        case DebugMode.Sync => "SYNC"
        case DebugMode.No   => "NO"
      }
  }

  object DebugMode {
    case object Yes  extends DebugMode
    case object Sync extends DebugMode
    case object No   extends DebugMode
  }
}
