package zio.redis

import zio._

package object internal {
  private[redis] def logScopeFinalizer(msg: String): URIO[Scope, Unit] =
    for {
      scope <- ZIO.scope
      _ <- scope.addFinalizerExit {
             case Exit.Success(_)  => ZIO.logTrace(s"$msg with success")
             case Exit.Failure(th) => ZIO.logTraceCause(s"$msg with failure", th)
           }
    } yield ()

}
