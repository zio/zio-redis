package zio.redis.options

trait SortedSets {

  sealed trait LexMinimum

  object LexMinimum {
    case object Unbounded                   extends LexMinimum
    sealed case class Open(value: String)   extends LexMinimum
    sealed case class Closed(value: String) extends LexMinimum
  }

  sealed trait LexMaximum

  object LexMaximum {
    case object Unbounded                   extends LexMaximum
    sealed case class Open(value: String)   extends LexMaximum
    sealed case class Closed(value: String) extends LexMaximum
  }

  sealed case class LexRange(min: LexMinimum, max: LexMaximum)

  sealed trait ScoreMinimum

  object ScoreMinimum {
    case object Infinity                    extends ScoreMinimum
    sealed case class Open(value: String)   extends ScoreMinimum
    sealed case class Closed(value: String) extends ScoreMinimum
  }

  sealed trait ScoreMaximum

  object ScoreMaximum {
    case object Infinity                    extends ScoreMaximum
    sealed case class Open(value: String)   extends ScoreMaximum
    sealed case class Closed(value: String) extends ScoreMaximum
  }

  sealed case class ScoreRange(min: ScoreMinimum, max: ScoreMaximum)

  sealed trait Aggregate

  object Aggregate {
    case object SUM extends Aggregate
    case object MIN extends Aggregate
    case object MAX extends Aggregate
  }

  sealed case class Limit(offset: Long, count: Long)

  case object WithScores
  type WithScores = WithScores.type

  case object Change
  type Change = Change.type

  case object Increment
  type Increment = Increment.type

  sealed case class MemberScore(score: Double, member: String)
}
