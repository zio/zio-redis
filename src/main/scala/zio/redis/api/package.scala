package zio.redis

package object api {
  lazy val connection  = new Connection  {}
  lazy val hashes      = new Hashes      {}
  lazy val hyperloglog = new HyperLogLog {}
  lazy val keys        = new Keys        {}
  lazy val lists       = new Lists       {}
  lazy val sets        = new Sets        {}
  lazy val sortedsets  = new SortedSets  {}
  lazy val strings     = new Strings     {}
}
