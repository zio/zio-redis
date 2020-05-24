package zio.redis.options

trait SortedSets {
  sealed trait Aggregate { self =>
    final def stringify: String =
      self match {
        case Aggregate.Max => "MAX"
        case Aggregate.Min => "MIN"
        case Aggregate.Sum => "SUM"
      }
  }

  object Aggregate {
    case object Sum extends Aggregate
    case object Min extends Aggregate
    case object Max extends Aggregate
  }

  case object Changed
  type Changed = Changed.type

  case object Increment
  type Increment = Increment.type

  sealed trait LexMaximum

  object LexMaximum {
    case object Unbounded                   extends LexMaximum
    sealed case class Open(value: String)   extends LexMaximum
    sealed case class Closed(value: String) extends LexMaximum
  }

  sealed trait LexMinimum

  object LexMinimum {
    case object Unbounded                   extends LexMinimum
    sealed case class Open(value: String)   extends LexMinimum
    sealed case class Closed(value: String) extends LexMinimum
  }

  sealed case class LexRange(min: LexMinimum, max: LexMaximum)

  sealed case class MemberScore(score: Double, member: String)

  sealed trait ScoreMaximum

  object ScoreMaximum {
    case object Infinity                    extends ScoreMaximum
    sealed case class Open(value: String)   extends ScoreMaximum
    sealed case class Closed(value: String) extends ScoreMaximum
  }

  sealed trait ScoreMinimum

  object ScoreMinimum {
    case object Infinity                    extends ScoreMinimum
    sealed case class Open(value: String)   extends ScoreMinimum
    sealed case class Closed(value: String) extends ScoreMinimum
  }

  sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum)

  case object WithScores
  type WithScores = WithScores.type
}
