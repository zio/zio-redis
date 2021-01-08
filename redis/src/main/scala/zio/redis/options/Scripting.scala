package zio.redis.options

trait Scripting {

  case class Script[K, A](lua: String, keys: Seq[K], args: Seq[A])
}
