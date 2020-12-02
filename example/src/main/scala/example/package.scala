import zio.Has

package object example {
  type ContributorsCache = Has[ContributorsCache.Service]
  type Contributions     = Contributions.Type
  type Login             = Login.Type
  type Organization      = Organization.Type
  type Name              = Name.Type
}
