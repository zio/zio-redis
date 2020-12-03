package example

final case class Repository(owner: Owner, name: Name) {
  lazy val key: String = s"$owner:$name"
}
