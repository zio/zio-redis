package zio.redis

final case class GeoRadius(val value: Double) extends AnyVal

object GeoRadius {
  def of(value: Double): Option[GeoRadius] =
    if (value < 0.0) None else Some(new GeoRadius(value))
}
