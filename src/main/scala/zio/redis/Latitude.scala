package zio.redis

final class Latitude private (val value: Double) extends AnyVal

object Latitude {
  def of(value: Double): Option[Latitude] =
    if (value < -85.05112878 || value > 85.05112878) None else Some(new Latitude(value))
}
