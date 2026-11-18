---
id: index
title: "Introduction to ZIO Redis"
sidebar_label: "Introduction"
---

@PROJECT_BADGES@

## Introduction

[ZIO Redis](https://github.com/zio/zio-redis) is a ZIO-native Redis client.
It aims to provide a **type-safe** and **performant** API for accessing Redis
instances.

## Installation

ZIO Redis is published for Scala 2.12, 2.13 and 3 on the JVM (Java 17 or later). Add it to your `build.sbt`:

```scala
libraryDependencies ++= Seq(
  // ZIO Redis client (JVM)
  "dev.zio" %% "zio-redis" % "@VERSION@"
)
```

Values are encoded and decoded by a `CodecSupplier`. The built-in `CodecSupplier.utf8` handles strings and numbers; to store case classes, add a ZIO Schema codec as described in [Getting Started](./getting-started.md#installation) and [Codecs](./concepts/codecs.md).

## Modules

### Testing

`zio-redis-embedded` starts a real Redis process for the duration of a test, so specs run without Docker or a pre-installed server. Use the same version as `zio-redis`. See [How to Test With Embedded Redis](./how-to/test-with-embedded-redis.md).

```scala
libraryDependencies ++= Seq(
  // Embedded Redis server for tests (JVM)
  "dev.zio" %% "zio-redis-embedded" % "@VERSION@" % Test
)
```

## Features

- **Type-safe commands.** Results are decoded to any type with a ZIO Schema `Schema`; see [Codecs](./codecs.md).
- **All major command groups.** Strings, keys, hashes, lists, sets, sorted sets, streams, geo, HyperLogLog and scripting; see the [command reference](./reference/commands/strings.md) pages.
- **Pub/Sub.** Subscriptions are exposed as ZIO streams; see [Pub/Sub](./pub-sub.md).
- **Pipelining.** `AsyncRedis` sends commands without waiting for replies; see [AsyncRedis](./async-redis.md).
- **Cluster support.** `Redis.cluster` follows slot redirections; see [Cluster](./cluster.md).
- **Embedded server for tests.** See [How to Test With Embedded Redis](./how-to/test-with-embedded-redis.md).

## Quick Example

To run an effect that uses Redis, provide the `Redis` layer. It needs a `CodecSupplier` and, for a local server, nothing else. See [Getting Started](./getting-started.md) for the details:

```scala mdoc:compile-only
import zio._
import zio.redis._

object QuickExample extends ZIOAppDefault {
  val myApp: ZIO[Redis, RedisError, Unit] =
    for {
      redis <- ZIO.service[Redis]
      _     <- redis.set("myKey", 8L, Some(1.minute))
      v     <- redis.get("myKey").returning[Long]
      _     <- ZIO.debug(s"Value of myKey: $v")
      _     <- redis.hSet("myHash", ("k1", 6), ("k2", 2))
      _     <- redis.rPush("myList", 1, 2, 3, 4)
      _     <- redis.sAdd("mySet", "a", "b", "a", "c")
    } yield ()

  def run =
    myApp.provide(Redis.local, ZLayer.succeed[CodecSupplier](CodecSupplier.utf8))
}
```
