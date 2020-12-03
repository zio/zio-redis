import zio.Has

package object example {
  type ContributorsCache = Has[ContributorsCache.Service]
  type Contributions     = Contributions.Type
  type Login             = Login.Type
  type Owner             = Owner.Type
  type Name              = Name.Type
}
