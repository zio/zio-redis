package zio.nio

import java.lang
import java.net.{ StandardSocketOptions => JSSOptions }

object StandardSocketOptions {

  val SO_KEEPALIVE: SocketOption[lang.Boolean] = new SocketOption[lang.Boolean](JSSOptions.SO_KEEPALIVE)
  val TCP_NODELAY: SocketOption[lang.Boolean]  = new SocketOption(JSSOptions.TCP_NODELAY)

}
