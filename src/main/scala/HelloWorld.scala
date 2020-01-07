import zio.blocking.Blocking
import zio.{ App, UIO, ZIO, ZManaged }
import zio.redis.{ R, RedisIO }
import zio.console._

object HelloWorld extends App {
  import R._

  val redis: ZManaged[Blocking with Console, Throwable, RedisIO] =
    RedisIO.make("localhost", 6379)

  def run(args: List[String]) =
    myAppLogic.foldM(ex => putStrLn(ex.getMessage).as(1), _ => UIO.succeed(0))

  val myAppLogic: ZIO[Console with Blocking, Throwable, Unit] = (for {
//    result <- hexists("user:authtoken", "paul@leadiq.com").provideSomeManaged(redis)
    result <- hgetall[String, String]("user:authtoken").provideSomeManaged(redis)
    _      <- putStrLn(result.toString)
//    _      <- ZIO.traverse_(result)(putStrLn)
  } yield ())
}
