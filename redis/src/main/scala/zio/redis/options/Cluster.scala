package zio.redis.options

trait Cluster {

  sealed trait ClusterState

  object ClusterState {
    final case object Ok   extends ClusterState
    final case object Fail extends ClusterState
  }

  sealed case class ClusterInfo(
    state: ClusterState,
    slotsAssigned: Int,
    slotsOk: Int,
    slotsPfail: Int,
    slotsFail: Int,
    knownNodes: Int,
    size: Int,
    currentEpoch: Int,
    myEpoch: Int,
    statsMessagesSent: Int,
    statsMessagesReceived: Int
  )
}
