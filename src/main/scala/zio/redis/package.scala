package zio

package object redis {
  import api._

  object all extends Connection with Hashes with HyperLogLog with Keys with Lists with Sets with Strings with SortedSets

  object connection extends Connection
  object hashes     extends Hashes
  object hll        extends HyperLogLog
  object keys       extends Keys
  object lists      extends Lists
  object sets       extends Sets
  object strings    extends Strings
  object zsets      extends SortedSets
}
