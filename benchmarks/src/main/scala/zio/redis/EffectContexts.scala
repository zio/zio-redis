package zio.redis

import cats.effect.{ ContextShift, Timer, IO => CatsIO }
import scala.concurrent.ExecutionContext

trait EffectContexts {
  implicit val cs: ContextShift[CatsIO] = CatsIO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[CatsIO]     = CatsIO.timer(ExecutionContext.global)
}
