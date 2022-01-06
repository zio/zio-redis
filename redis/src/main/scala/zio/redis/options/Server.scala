package zio.redis.options

trait Server {

  sealed trait AclCategory { self =>
    private[redis] final def stringify: String =
      self match {
        case AclCategory.Keyspace    => "keyspace"
        case AclCategory.Read        => "read"
        case AclCategory.Write       => "write"
        case AclCategory.Set         => "set"
        case AclCategory.Sortedset   => "sortedset"
        case AclCategory.List        => "list"
        case AclCategory.Hash        => "hash"
        case AclCategory.String      => "string"
        case AclCategory.Bitmap      => "bitmap"
        case AclCategory.Hyperloglog => "hyperloglog"
        case AclCategory.Geo         => "geo"
        case AclCategory.Stream      => "stream"
        case AclCategory.Pubsub      => "pubsub"
        case AclCategory.Admin       => "admin"
        case AclCategory.Fast        => "fast"
        case AclCategory.Slow        => "slow"
        case AclCategory.Blocking    => "blocking"
        case AclCategory.Dangerous   => "dangerous"
        case AclCategory.Connection  => "connection"
        case AclCategory.Transaction => "transaction"
        case AclCategory.Scripting   => "scripting"
      }
  }

  object AclCategory {
    def unapply(category: String): Option[AclCategory] = category match {
      case "keyspace"    => Some(Keyspace)
      case "read"        => Some(Read)
      case "write"       => Some(Write)
      case "set"         => Some(Set)
      case "sortedset"   => Some(Sortedset)
      case "list"        => Some(List)
      case "hash"        => Some(Hash)
      case "string"      => Some(String)
      case "bitmap"      => Some(Bitmap)
      case "hyperloglog" => Some(Hyperloglog)
      case "geo"         => Some(Geo)
      case "stream"      => Some(Stream)
      case "pubsub"      => Some(Pubsub)
      case "admin"       => Some(Admin)
      case "fast"        => Some(Fast)
      case "slow"        => Some(Slow)
      case "blocking"    => Some(Blocking)
      case "dangerous"   => Some(Dangerous)
      case "connection"  => Some(Connection)
      case "transaction" => Some(Transaction)
      case "scripting"   => Some(Scripting)
      case _             => None
    }

    case object Keyspace    extends AclCategory
    case object Read        extends AclCategory
    case object Write       extends AclCategory
    case object Set         extends AclCategory
    case object Sortedset   extends AclCategory
    case object List        extends AclCategory
    case object Hash        extends AclCategory
    case object String      extends AclCategory
    case object Bitmap      extends AclCategory
    case object Hyperloglog extends AclCategory
    case object Geo         extends AclCategory
    case object Stream      extends AclCategory
    case object Pubsub      extends AclCategory
    case object Admin       extends AclCategory
    case object Fast        extends AclCategory
    case object Slow        extends AclCategory
    case object Blocking    extends AclCategory
    case object Dangerous   extends AclCategory
    case object Connection  extends AclCategory
    case object Transaction extends AclCategory
    case object Scripting   extends AclCategory
  }

  sealed trait ServerCommand { self =>
    private[redis] final def stringify: String =
      self match {
        case ServerCommand.Restore              => "restore"
        case ServerCommand.Renamenx             => "renamenx"
        case ServerCommand.Touch                => "touch"
        case ServerCommand.Type                 => "type"
        case ServerCommand.Dbsize               => "dbsize"
        case ServerCommand.Keys                 => "keys"
        case ServerCommand.Pexpire              => "pexpire"
        case ServerCommand.Exists               => "exists"
        case ServerCommand.Expireat             => "expireat"
        case ServerCommand.Dump                 => "dump"
        case ServerCommand.Copy                 => "copy"
        case ServerCommand.Object               => "object"
        case ServerCommand.Pttl                 => "pttl"
        case ServerCommand.Del                  => "del"
        case ServerCommand.Readonly             => "readonly"
        case ServerCommand.Ttl                  => "ttl"
        case ServerCommand.Move                 => "move"
        case ServerCommand.Flushall             => "flushall"
        case ServerCommand.Readwrite            => "readwrite"
        case ServerCommand.Randomkey            => "randomkey"
        case ServerCommand.Rename               => "rename"
        case ServerCommand.Pexpireat            => "pexpireat"
        case ServerCommand.Expire               => "expire"
        case ServerCommand.Flushdb              => "flushdb"
        case ServerCommand.Wait                 => "wait"
        case ServerCommand.Asking               => "asking"
        case ServerCommand.Select               => "select"
        case ServerCommand.Swapdb               => "swapdb"
        case ServerCommand.Persist              => "persist"
        case ServerCommand.RestoreAsking        => "restore-asking"
        case ServerCommand.Scan                 => "scan"
        case ServerCommand.Unlink               => "unlink"
        case ServerCommand.Migrate              => "migrate"
        case ServerCommand.Host                 => "host:"
        case ServerCommand.Geodist              => "geodist"
        case ServerCommand.Zrange               => "zrange"
        case ServerCommand.Scard                => "scard"
        case ServerCommand.Sscan                => "sscan"
        case ServerCommand.Xrange               => "xrange"
        case ServerCommand.Get                  => "get"
        case ServerCommand.Zrevrange            => "zrevrange"
        case ServerCommand.Mget                 => "mget"
        case ServerCommand.Bitpos               => "bitpos"
        case ServerCommand.Sunion               => "sunion"
        case ServerCommand.Zrevrangebylex       => "zrevrangebylex"
        case ServerCommand.Hvals                => "hvals"
        case ServerCommand.Zcount               => "zcount"
        case ServerCommand.Zcard                => "zcard"
        case ServerCommand.Hstrlen              => "hstrlen"
        case ServerCommand.Lolwut               => "lolwut"
        case ServerCommand.Lpos                 => "lpos"
        case ServerCommand.Smismember           => "smismember"
        case ServerCommand.Sinter               => "sinter"
        case ServerCommand.Pfcount              => "pfcount"
        case ServerCommand.Hget                 => "hget"
        case ServerCommand.Geosearch            => "geosearch"
        case ServerCommand.Xrevrange            => "xrevrange"
        case ServerCommand.Georadius_ro         => "georadius_ro"
        case ServerCommand.Hkeys                => "hkeys"
        case ServerCommand.Zrank                => "zrank"
        case ServerCommand.Lindex               => "lindex"
        case ServerCommand.Geohash              => "geohash"
        case ServerCommand.Zrangebyscore        => "zrangebyscore"
        case ServerCommand.Hgetall              => "hgetall"
        case ServerCommand.Sdiff                => "sdiff"
        case ServerCommand.Hscan                => "hscan"
        case ServerCommand.Post                 => "post"
        case ServerCommand.Zscan                => "zscan"
        case ServerCommand.Stralgo              => "stralgo"
        case ServerCommand.Llen                 => "llen"
        case ServerCommand.Lrange               => "lrange"
        case ServerCommand.Bitcount             => "bitcount"
        case ServerCommand.Zscore               => "zscore"
        case ServerCommand.Srandmember          => "srandmember"
        case ServerCommand.Bitfield_ro          => "bitfield_ro"
        case ServerCommand.Zunion               => "zunion"
        case ServerCommand.Zrangebylex          => "zrangebylex"
        case ServerCommand.Getbit               => "getbit"
        case ServerCommand.Zlexcount            => "zlexcount"
        case ServerCommand.Georadiusbymember_ro => "georadiusbymember_ro"
        case ServerCommand.Xread                => "xread"
        case ServerCommand.Zinter               => "zinter"
        case ServerCommand.Smembers             => "smembers"
        case ServerCommand.Zdiff                => "zdiff"
        case ServerCommand.Geopos               => "geopos"
        case ServerCommand.Memory               => "memory"
        case ServerCommand.Zmscore              => "zmscore"
        case ServerCommand.Substr               => "substr"
        case ServerCommand.Xpending             => "xpending"
        case ServerCommand.Zrevrangebyscore     => "zrevrangebyscore"
        case ServerCommand.Xinfo                => "xinfo"
        case ServerCommand.Zrevrank             => "zrevrank"
        case ServerCommand.Zrandmember          => "zrandmember"
        case ServerCommand.Hlen                 => "hlen"
        case ServerCommand.Xlen                 => "xlen"
        case ServerCommand.Getrange             => "getrange"
        case ServerCommand.Hrandfield           => "hrandfield"
        case ServerCommand.Hmget                => "hmget"
        case ServerCommand.Strlen               => "strlen"
        case ServerCommand.Hexists              => "hexists"
        case ServerCommand.Sismember            => "sismember"
        case ServerCommand.Smove                => "smove"
        case ServerCommand.Zinterstore          => "zinterstore"
        case ServerCommand.Setbit               => "setbit"
        case ServerCommand.Hmset                => "hmset"
        case ServerCommand.Append               => "append"
        case ServerCommand.Rpoplpush            => "rpoplpush"
        case ServerCommand.Blmove               => "blmove"
        case ServerCommand.Pfdebug              => "pfdebug"
        case ServerCommand.Mset                 => "mset"
        case ServerCommand.Bitfield             => "bitfield"
        case ServerCommand.Getdel               => "getdel"
        case ServerCommand.Lpushx               => "lpushx"
        case ServerCommand.Srem                 => "srem"
        case ServerCommand.Hincrby              => "hincrby"
        case ServerCommand.Rpop                 => "rpop"
        case ServerCommand.Geoadd               => "geoadd"
        case ServerCommand.Incr                 => "incr"
        case ServerCommand.Zunionstore          => "zunionstore"
        case ServerCommand.Bzpopmax             => "bzpopmax"
        case ServerCommand.Geosearchstore       => "geosearchstore"
        case ServerCommand.Bitop                => "bitop"
        case ServerCommand.Incrbyfloat          => "incrbyfloat"
        case ServerCommand.Zdiffstore           => "zdiffstore"
        case ServerCommand.Setnx                => "setnx"
        case ServerCommand.Pfadd                => "pfadd"
        case ServerCommand.Hincrbyfloat         => "hincrbyfloat"
        case ServerCommand.Lpush                => "lpush"
        case ServerCommand.Xadd                 => "xadd"
        case ServerCommand.Zremrangebylex       => "zremrangebylex"
        case ServerCommand.Set                  => "set"
        case ServerCommand.Msetnx               => "msetnx"
        case ServerCommand.Sadd                 => "sadd"
        case ServerCommand.Rpushx               => "rpushx"
        case ServerCommand.Lrem                 => "lrem"
        case ServerCommand.Setex                => "setex"
        case ServerCommand.Zremrangebyscore     => "zremrangebyscore"
        case ServerCommand.Xtrim                => "xtrim"
        case ServerCommand.Sunionstore          => "sunionstore"
        case ServerCommand.Xack                 => "xack"
        case ServerCommand.Hset                 => "hset"
        case ServerCommand.Brpoplpush           => "brpoplpush"
        case ServerCommand.Georadius            => "georadius"
        case ServerCommand.Lset                 => "lset"
        case ServerCommand.Ltrim                => "ltrim"
        case ServerCommand.Sort                 => "sort"
        case ServerCommand.Xreadgroup           => "xreadgroup"
        case ServerCommand.Xdel                 => "xdel"
        case ServerCommand.Lmove                => "lmove"
        case ServerCommand.Zrem                 => "zrem"
        case ServerCommand.Xgroup               => "xgroup"
        case ServerCommand.Zpopmax              => "zpopmax"
        case ServerCommand.Lpop                 => "lpop"
        case ServerCommand.Zincrby              => "zincrby"
        case ServerCommand.Brpop                => "brpop"
        case ServerCommand.Bzpopmin             => "bzpopmin"
        case ServerCommand.Rpush                => "rpush"
        case ServerCommand.Sdiffstore           => "sdiffstore"
        case ServerCommand.Decr                 => "decr"
        case ServerCommand.Hsetnx               => "hsetnx"
        case ServerCommand.Psetex               => "psetex"
        case ServerCommand.Georadiusbymember    => "georadiusbymember"
        case ServerCommand.Zpopmin              => "zpopmin"
        case ServerCommand.Xsetid               => "xsetid"
        case ServerCommand.Zrangestore          => "zrangestore"
        case ServerCommand.Getex                => "getex"
        case ServerCommand.Spop                 => "spop"
        case ServerCommand.Pfmerge              => "pfmerge"
        case ServerCommand.Zadd                 => "zadd"
        case ServerCommand.Xclaim               => "xclaim"
        case ServerCommand.Blpop                => "blpop"
        case ServerCommand.Hdel                 => "hdel"
        case ServerCommand.Setrange             => "setrange"
        case ServerCommand.Linsert              => "linsert"
        case ServerCommand.Zremrangebyrank      => "zremrangebyrank"
        case ServerCommand.Sinterstore          => "sinterstore"
        case ServerCommand.Incrby               => "incrby"
        case ServerCommand.Xautoclaim           => "xautoclaim"
        case ServerCommand.Decrby               => "decrby"
        case ServerCommand.Getset               => "getset"
        case ServerCommand.Pfselftest           => "pfselftest"
        case ServerCommand.Psubscribe           => "psubscribe"
        case ServerCommand.Punsubscribe         => "punsubscribe"
        case ServerCommand.Unsubscribe          => "unsubscribe"
        case ServerCommand.Publish              => "publish"
        case ServerCommand.Pubsub               => "pubsub"
        case ServerCommand.Subscribe            => "subscribe"
        case ServerCommand.Module               => "module"
        case ServerCommand.Sync                 => "sync"
        case ServerCommand.Acl                  => "acl"
        case ServerCommand.Psync                => "psync"
        case ServerCommand.Slowlog              => "slowlog"
        case ServerCommand.Bgrewriteaof         => "bgrewriteaof"
        case ServerCommand.Shutdown             => "shutdown"
        case ServerCommand.Latency              => "latency"
        case ServerCommand.Debug                => "debug"
        case ServerCommand.Replicaof            => "replicaof"
        case ServerCommand.Cluster              => "cluster"
        case ServerCommand.Slaveof              => "slaveof"
        case ServerCommand.Client               => "client"
        case ServerCommand.Monitor              => "monitor"
        case ServerCommand.Replconf             => "replconf"
        case ServerCommand.Bgsave               => "bgsave"
        case ServerCommand.Config               => "config"
        case ServerCommand.Failover             => "failover"
        case ServerCommand.Lastsave             => "lastsave"
        case ServerCommand.Save                 => "save"
        case ServerCommand.Auth                 => "auth"
        case ServerCommand.Hello                => "hello"
        case ServerCommand.Echo                 => "echo"
        case ServerCommand.Discard              => "discard"
        case ServerCommand.Time                 => "time"
        case ServerCommand.Role                 => "role"
        case ServerCommand.Multi                => "multi"
        case ServerCommand.Watch                => "watch"
        case ServerCommand.Reset                => "reset"
        case ServerCommand.Ping                 => "ping"
        case ServerCommand.Unwatch              => "unwatch"
        case ServerCommand.Info                 => "info"
        case ServerCommand.Evalsha              => "evalsha"
        case ServerCommand.Command              => "command"
        case ServerCommand.Script               => "script"
        case ServerCommand.Eval                 => "eval"
        case ServerCommand.Exec                 => "exec"
      }
  }

  object ServerCommand {
    def unapply(command: String): Option[ServerCommand] = command match {
      case "restore"              => Some(Restore)
      case "renamenx"             => Some(Renamenx)
      case "touch"                => Some(Touch)
      case "type"                 => Some(Type)
      case "dbsize"               => Some(Dbsize)
      case "keys"                 => Some(Keys)
      case "pexpire"              => Some(Pexpire)
      case "exists"               => Some(Exists)
      case "expireat"             => Some(Expireat)
      case "dump"                 => Some(Dump)
      case "copy"                 => Some(Copy)
      case "object"               => Some(Object)
      case "pttl"                 => Some(Pttl)
      case "del"                  => Some(Del)
      case "readonly"             => Some(Readonly)
      case "ttl"                  => Some(Ttl)
      case "move"                 => Some(Move)
      case "flushall"             => Some(Flushall)
      case "readwrite"            => Some(Readwrite)
      case "randomkey"            => Some(Randomkey)
      case "rename"               => Some(Rename)
      case "pexpireat"            => Some(Pexpireat)
      case "expire"               => Some(Expire)
      case "flushdb"              => Some(Flushdb)
      case "wait"                 => Some(Wait)
      case "asking"               => Some(Asking)
      case "select"               => Some(Select)
      case "swapdb"               => Some(Swapdb)
      case "persist"              => Some(Persist)
      case "restore-asking"       => Some(RestoreAsking)
      case "scan"                 => Some(Scan)
      case "unlink"               => Some(Unlink)
      case "migrate"              => Some(Migrate)
      case "host:"                => Some(Host)
      case "geodist"              => Some(Geodist)
      case "zrange"               => Some(Zrange)
      case "scard"                => Some(Scard)
      case "sscan"                => Some(Sscan)
      case "xrange"               => Some(Xrange)
      case "get"                  => Some(Get)
      case "zrevrange"            => Some(Zrevrange)
      case "mget"                 => Some(Mget)
      case "bitpos"               => Some(Bitpos)
      case "sunion"               => Some(Sunion)
      case "zrevrangebylex"       => Some(Zrevrangebylex)
      case "hvals"                => Some(Hvals)
      case "zcount"               => Some(Zcount)
      case "zcard"                => Some(Zcard)
      case "hstrlen"              => Some(Hstrlen)
      case "lolwut"               => Some(Lolwut)
      case "lpos"                 => Some(Lpos)
      case "smismember"           => Some(Smismember)
      case "sinter"               => Some(Sinter)
      case "pfcount"              => Some(Pfcount)
      case "hget"                 => Some(Hget)
      case "geosearch"            => Some(Geosearch)
      case "xrevrange"            => Some(Xrevrange)
      case "georadius_ro"         => Some(Georadius_ro)
      case "hkeys"                => Some(Hkeys)
      case "zrank"                => Some(Zrank)
      case "lindex"               => Some(Lindex)
      case "geohash"              => Some(Geohash)
      case "zrangebyscore"        => Some(Zrangebyscore)
      case "hgetall"              => Some(Hgetall)
      case "sdiff"                => Some(Sdiff)
      case "hscan"                => Some(Hscan)
      case "post"                 => Some(Post)
      case "zscan"                => Some(Zscan)
      case "stralgo"              => Some(Stralgo)
      case "llen"                 => Some(Llen)
      case "lrange"               => Some(Lrange)
      case "bitcount"             => Some(Bitcount)
      case "zscore"               => Some(Zscore)
      case "srandmember"          => Some(Srandmember)
      case "bitfield_ro"          => Some(Bitfield_ro)
      case "zunion"               => Some(Zunion)
      case "zrangebylex"          => Some(Zrangebylex)
      case "getbit"               => Some(Getbit)
      case "zlexcount"            => Some(Zlexcount)
      case "georadiusbymember_ro" => Some(Georadiusbymember_ro)
      case "xread"                => Some(Xread)
      case "zinter"               => Some(Zinter)
      case "smembers"             => Some(Smembers)
      case "zdiff"                => Some(Zdiff)
      case "geopos"               => Some(Geopos)
      case "memory"               => Some(Memory)
      case "zmscore"              => Some(Zmscore)
      case "substr"               => Some(Substr)
      case "xpending"             => Some(Xpending)
      case "zrevrangebyscore"     => Some(Zrevrangebyscore)
      case "xinfo"                => Some(Xinfo)
      case "zrevrank"             => Some(Zrevrank)
      case "zrandmember"          => Some(Zrandmember)
      case "hlen"                 => Some(Hlen)
      case "xlen"                 => Some(Xlen)
      case "getrange"             => Some(Getrange)
      case "hrandfield"           => Some(Hrandfield)
      case "hmget"                => Some(Hmget)
      case "strlen"               => Some(Strlen)
      case "hexists"              => Some(Hexists)
      case "sismember"            => Some(Sismember)
      case "smove"                => Some(Smove)
      case "zinterstore"          => Some(Zinterstore)
      case "setbit"               => Some(Setbit)
      case "hmset"                => Some(Hmset)
      case "append"               => Some(Append)
      case "rpoplpush"            => Some(Rpoplpush)
      case "blmove"               => Some(Blmove)
      case "pfdebug"              => Some(Pfdebug)
      case "mset"                 => Some(Mset)
      case "bitfield"             => Some(Bitfield)
      case "getdel"               => Some(Getdel)
      case "lpushx"               => Some(Lpushx)
      case "srem"                 => Some(Srem)
      case "hincrby"              => Some(Hincrby)
      case "rpop"                 => Some(Rpop)
      case "geoadd"               => Some(Geoadd)
      case "incr"                 => Some(Incr)
      case "zunionstore"          => Some(Zunionstore)
      case "bzpopmax"             => Some(Bzpopmax)
      case "geosearchstore"       => Some(Geosearchstore)
      case "bitop"                => Some(Bitop)
      case "incrbyfloat"          => Some(Incrbyfloat)
      case "zdiffstore"           => Some(Zdiffstore)
      case "setnx"                => Some(Setnx)
      case "pfadd"                => Some(Pfadd)
      case "hincrbyfloat"         => Some(Hincrbyfloat)
      case "lpush"                => Some(Lpush)
      case "xadd"                 => Some(Xadd)
      case "zremrangebylex"       => Some(Zremrangebylex)
      case "set"                  => Some(Set)
      case "msetnx"               => Some(Msetnx)
      case "sadd"                 => Some(Sadd)
      case "rpushx"               => Some(Rpushx)
      case "lrem"                 => Some(Lrem)
      case "setex"                => Some(Setex)
      case "zremrangebyscore"     => Some(Zremrangebyscore)
      case "xtrim"                => Some(Xtrim)
      case "sunionstore"          => Some(Sunionstore)
      case "xack"                 => Some(Xack)
      case "hset"                 => Some(Hset)
      case "brpoplpush"           => Some(Brpoplpush)
      case "georadius"            => Some(Georadius)
      case "lset"                 => Some(Lset)
      case "ltrim"                => Some(Ltrim)
      case "sort"                 => Some(Sort)
      case "xreadgroup"           => Some(Xreadgroup)
      case "xdel"                 => Some(Xdel)
      case "lmove"                => Some(Lmove)
      case "zrem"                 => Some(Zrem)
      case "xgroup"               => Some(Xgroup)
      case "zpopmax"              => Some(Zpopmax)
      case "lpop"                 => Some(Lpop)
      case "zincrby"              => Some(Zincrby)
      case "brpop"                => Some(Brpop)
      case "bzpopmin"             => Some(Bzpopmin)
      case "rpush"                => Some(Rpush)
      case "sdiffstore"           => Some(Sdiffstore)
      case "decr"                 => Some(Decr)
      case "hsetnx"               => Some(Hsetnx)
      case "psetex"               => Some(Psetex)
      case "georadiusbymember"    => Some(Georadiusbymember)
      case "zpopmin"              => Some(Zpopmin)
      case "xsetid"               => Some(Xsetid)
      case "zrangestore"          => Some(Zrangestore)
      case "getex"                => Some(Getex)
      case "spop"                 => Some(Spop)
      case "pfmerge"              => Some(Pfmerge)
      case "zadd"                 => Some(Zadd)
      case "xclaim"               => Some(Xclaim)
      case "blpop"                => Some(Blpop)
      case "hdel"                 => Some(Hdel)
      case "setrange"             => Some(Setrange)
      case "linsert"              => Some(Linsert)
      case "zremrangebyrank"      => Some(Zremrangebyrank)
      case "sinterstore"          => Some(Sinterstore)
      case "incrby"               => Some(Incrby)
      case "xautoclaim"           => Some(Xautoclaim)
      case "decrby"               => Some(Decrby)
      case "getset"               => Some(Getset)
      case "pfselftest"           => Some(Pfselftest)
      case "psubscribe"           => Some(Psubscribe)
      case "punsubscribe"         => Some(Punsubscribe)
      case "unsubscribe"          => Some(Unsubscribe)
      case "publish"              => Some(Publish)
      case "pubsub"               => Some(Pubsub)
      case "subscribe"            => Some(Subscribe)
      case "module"               => Some(Module)
      case "sync"                 => Some(Sync)
      case "acl"                  => Some(Acl)
      case "psync"                => Some(Psync)
      case "slowlog"              => Some(Slowlog)
      case "bgrewriteaof"         => Some(Bgrewriteaof)
      case "shutdown"             => Some(Shutdown)
      case "latency"              => Some(Latency)
      case "debug"                => Some(Debug)
      case "replicaof"            => Some(Replicaof)
      case "cluster"              => Some(Cluster)
      case "slaveof"              => Some(Slaveof)
      case "client"               => Some(Client)
      case "monitor"              => Some(Monitor)
      case "replconf"             => Some(Replconf)
      case "bgsave"               => Some(Bgsave)
      case "config"               => Some(Config)
      case "failover"             => Some(Failover)
      case "lastsave"             => Some(Lastsave)
      case "save"                 => Some(Save)
      case "auth"                 => Some(Auth)
      case "hello"                => Some(Hello)
      case "echo"                 => Some(Echo)
      case "discard"              => Some(Discard)
      case "time"                 => Some(Time)
      case "role"                 => Some(Role)
      case "multi"                => Some(Multi)
      case "watch"                => Some(Watch)
      case "reset"                => Some(Reset)
      case "ping"                 => Some(Ping)
      case "unwatch"              => Some(Unwatch)
      case "info"                 => Some(Info)
      case "evalsha"              => Some(Evalsha)
      case "command"              => Some(Command)
      case "script"               => Some(Script)
      case "eval"                 => Some(Eval)
      case "exec"                 => Some(Exec)
      case _ => None
    }

    case object Restore              extends ServerCommand
    case object Renamenx             extends ServerCommand
    case object Touch                extends ServerCommand
    case object Type                 extends ServerCommand
    case object Dbsize               extends ServerCommand
    case object Keys                 extends ServerCommand
    case object Pexpire              extends ServerCommand
    case object Exists               extends ServerCommand
    case object Expireat             extends ServerCommand
    case object Dump                 extends ServerCommand
    case object Copy                 extends ServerCommand
    case object Object               extends ServerCommand
    case object Pttl                 extends ServerCommand
    case object Del                  extends ServerCommand
    case object Readonly             extends ServerCommand
    case object Ttl                  extends ServerCommand
    case object Move                 extends ServerCommand
    case object Flushall             extends ServerCommand
    case object Readwrite            extends ServerCommand
    case object Randomkey            extends ServerCommand
    case object Rename               extends ServerCommand
    case object Pexpireat            extends ServerCommand
    case object Expire               extends ServerCommand
    case object Flushdb              extends ServerCommand
    case object Wait                 extends ServerCommand
    case object Asking               extends ServerCommand
    case object Select               extends ServerCommand
    case object Swapdb               extends ServerCommand
    case object Persist              extends ServerCommand
    case object RestoreAsking        extends ServerCommand
    case object Scan                 extends ServerCommand
    case object Unlink               extends ServerCommand
    case object Migrate              extends ServerCommand
    case object Host                 extends ServerCommand
    case object Geodist              extends ServerCommand
    case object Zrange               extends ServerCommand
    case object Scard                extends ServerCommand
    case object Sscan                extends ServerCommand
    case object Xrange               extends ServerCommand
    case object Get                  extends ServerCommand
    case object Zrevrange            extends ServerCommand
    case object Mget                 extends ServerCommand
    case object Bitpos               extends ServerCommand
    case object Sunion               extends ServerCommand
    case object Zrevrangebylex       extends ServerCommand
    case object Hvals                extends ServerCommand
    case object Zcount               extends ServerCommand
    case object Zcard                extends ServerCommand
    case object Hstrlen              extends ServerCommand
    case object Lolwut               extends ServerCommand
    case object Lpos                 extends ServerCommand
    case object Smismember           extends ServerCommand
    case object Sinter               extends ServerCommand
    case object Pfcount              extends ServerCommand
    case object Hget                 extends ServerCommand
    case object Geosearch            extends ServerCommand
    case object Xrevrange            extends ServerCommand
    case object Georadius_ro         extends ServerCommand
    case object Hkeys                extends ServerCommand
    case object Zrank                extends ServerCommand
    case object Lindex               extends ServerCommand
    case object Geohash              extends ServerCommand
    case object Zrangebyscore        extends ServerCommand
    case object Hgetall              extends ServerCommand
    case object Sdiff                extends ServerCommand
    case object Hscan                extends ServerCommand
    case object Post                 extends ServerCommand
    case object Zscan                extends ServerCommand
    case object Stralgo              extends ServerCommand
    case object Llen                 extends ServerCommand
    case object Lrange               extends ServerCommand
    case object Bitcount             extends ServerCommand
    case object Zscore               extends ServerCommand
    case object Srandmember          extends ServerCommand
    case object Bitfield_ro          extends ServerCommand
    case object Zunion               extends ServerCommand
    case object Zrangebylex          extends ServerCommand
    case object Getbit               extends ServerCommand
    case object Zlexcount            extends ServerCommand
    case object Georadiusbymember_ro extends ServerCommand
    case object Xread                extends ServerCommand
    case object Zinter               extends ServerCommand
    case object Smembers             extends ServerCommand
    case object Zdiff                extends ServerCommand
    case object Geopos               extends ServerCommand
    case object Memory               extends ServerCommand
    case object Zmscore              extends ServerCommand
    case object Substr               extends ServerCommand
    case object Xpending             extends ServerCommand
    case object Zrevrangebyscore     extends ServerCommand
    case object Xinfo                extends ServerCommand
    case object Zrevrank             extends ServerCommand
    case object Zrandmember          extends ServerCommand
    case object Hlen                 extends ServerCommand
    case object Xlen                 extends ServerCommand
    case object Getrange             extends ServerCommand
    case object Hrandfield           extends ServerCommand
    case object Hmget                extends ServerCommand
    case object Strlen               extends ServerCommand
    case object Hexists              extends ServerCommand
    case object Sismember            extends ServerCommand
    case object Smove                extends ServerCommand
    case object Zinterstore          extends ServerCommand
    case object Setbit               extends ServerCommand
    case object Hmset                extends ServerCommand
    case object Append               extends ServerCommand
    case object Rpoplpush            extends ServerCommand
    case object Blmove               extends ServerCommand
    case object Pfdebug              extends ServerCommand
    case object Mset                 extends ServerCommand
    case object Bitfield             extends ServerCommand
    case object Getdel               extends ServerCommand
    case object Lpushx               extends ServerCommand
    case object Srem                 extends ServerCommand
    case object Hincrby              extends ServerCommand
    case object Rpop                 extends ServerCommand
    case object Geoadd               extends ServerCommand
    case object Incr                 extends ServerCommand
    case object Zunionstore          extends ServerCommand
    case object Bzpopmax             extends ServerCommand
    case object Geosearchstore       extends ServerCommand
    case object Bitop                extends ServerCommand
    case object Incrbyfloat          extends ServerCommand
    case object Zdiffstore           extends ServerCommand
    case object Setnx                extends ServerCommand
    case object Pfadd                extends ServerCommand
    case object Hincrbyfloat         extends ServerCommand
    case object Lpush                extends ServerCommand
    case object Xadd                 extends ServerCommand
    case object Zremrangebylex       extends ServerCommand
    case object Set                  extends ServerCommand
    case object Msetnx               extends ServerCommand
    case object Sadd                 extends ServerCommand
    case object Rpushx               extends ServerCommand
    case object Lrem                 extends ServerCommand
    case object Setex                extends ServerCommand
    case object Zremrangebyscore     extends ServerCommand
    case object Xtrim                extends ServerCommand
    case object Sunionstore          extends ServerCommand
    case object Xack                 extends ServerCommand
    case object Hset                 extends ServerCommand
    case object Brpoplpush           extends ServerCommand
    case object Georadius            extends ServerCommand
    case object Lset                 extends ServerCommand
    case object Ltrim                extends ServerCommand
    case object Sort                 extends ServerCommand
    case object Xreadgroup           extends ServerCommand
    case object Xdel                 extends ServerCommand
    case object Lmove                extends ServerCommand
    case object Zrem                 extends ServerCommand
    case object Xgroup               extends ServerCommand
    case object Zpopmax              extends ServerCommand
    case object Lpop                 extends ServerCommand
    case object Zincrby              extends ServerCommand
    case object Brpop                extends ServerCommand
    case object Bzpopmin             extends ServerCommand
    case object Rpush                extends ServerCommand
    case object Sdiffstore           extends ServerCommand
    case object Decr                 extends ServerCommand
    case object Hsetnx               extends ServerCommand
    case object Psetex               extends ServerCommand
    case object Georadiusbymember    extends ServerCommand
    case object Zpopmin              extends ServerCommand
    case object Xsetid               extends ServerCommand
    case object Zrangestore          extends ServerCommand
    case object Getex                extends ServerCommand
    case object Spop                 extends ServerCommand
    case object Pfmerge              extends ServerCommand
    case object Zadd                 extends ServerCommand
    case object Xclaim               extends ServerCommand
    case object Blpop                extends ServerCommand
    case object Hdel                 extends ServerCommand
    case object Setrange             extends ServerCommand
    case object Linsert              extends ServerCommand
    case object Zremrangebyrank      extends ServerCommand
    case object Sinterstore          extends ServerCommand
    case object Incrby               extends ServerCommand
    case object Xautoclaim           extends ServerCommand
    case object Decrby               extends ServerCommand
    case object Getset               extends ServerCommand
    case object Pfselftest           extends ServerCommand
    case object Psubscribe           extends ServerCommand
    case object Punsubscribe         extends ServerCommand
    case object Unsubscribe          extends ServerCommand
    case object Publish              extends ServerCommand
    case object Pubsub               extends ServerCommand
    case object Subscribe            extends ServerCommand
    case object Module               extends ServerCommand
    case object Sync                 extends ServerCommand
    case object Acl                  extends ServerCommand
    case object Psync                extends ServerCommand
    case object Slowlog              extends ServerCommand
    case object Bgrewriteaof         extends ServerCommand
    case object Shutdown             extends ServerCommand
    case object Latency              extends ServerCommand
    case object Debug                extends ServerCommand
    case object Replicaof            extends ServerCommand
    case object Cluster              extends ServerCommand
    case object Slaveof              extends ServerCommand
    case object Client               extends ServerCommand
    case object Monitor              extends ServerCommand
    case object Replconf             extends ServerCommand
    case object Bgsave               extends ServerCommand
    case object Config               extends ServerCommand
    case object Failover             extends ServerCommand
    case object Lastsave             extends ServerCommand
    case object Save                 extends ServerCommand
    case object Auth                 extends ServerCommand
    case object Hello                extends ServerCommand
    case object Echo                 extends ServerCommand
    case object Discard              extends ServerCommand
    case object Time                 extends ServerCommand
    case object Role                 extends ServerCommand
    case object Multi                extends ServerCommand
    case object Watch                extends ServerCommand
    case object Reset                extends ServerCommand
    case object Ping                 extends ServerCommand
    case object Unwatch              extends ServerCommand
    case object Info                 extends ServerCommand
    case object Evalsha              extends ServerCommand
    case object Command              extends ServerCommand
    case object Script               extends ServerCommand
    case object Eval                 extends ServerCommand
    case object Exec                 extends ServerCommand
  }

  sealed trait ServerRule

  object ServerRule {
    case object On extends ServerRule
    case object Off extends ServerRule

    case class KeyPattern(pattern: String) extends ServerRule
    case object ResetKeys extends ServerRule

    case class ChannelPattern(pattern: String) extends ServerRule // Channles also known as pubSub
    case object RestChannels extends ServerRule

    case class AddCommand(command: ServerCommand) extends ServerRule
    case class AddCategory(category: AclCategory) extends ServerRule

    

    val allKeys = KeyPattern("*")
    val allChannels = ChannelPattern("*")
  }
}
