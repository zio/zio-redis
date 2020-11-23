package example

import zio.Has

package object domain {
  type ContributorService = Has[ContributorService.Service]
}
