package example

final case class Repository(organization: Organization, name: Name) {
  lazy val key: String = s"$organization:$name"
}
