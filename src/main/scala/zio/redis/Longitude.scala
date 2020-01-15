package zio.redis

final class Longitude private (val value: Double) extends AnyVal

object Longitude {
  def of(value: Double): Option[Longitude] =
    if (value < -180.0 || value > 180.0) None else Some(new Longitude(value))
}
