---
id: index
title: "Introduction to ZIO Redis"
sidebar_label: "ZIO Redis"
---

@PROJECT_BADGES@

## Introduction

[ZIO Redis](https://github.com/zio/zio-redis) is a ZIO-native Redis client.
It aims to provide a **type-safe** and **performant** API for accessing Redis
instances.

## Installation

To use ZIO Redis, add the following line to your `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-redis" % "@VERSION@"
```

## Features

- **Type-safe commands.** Results are decoded to any type with a ZIO Schema `Schema`; see [Codecs](./codecs.md).
- **All major command groups.** Strings, keys, hashes, lists, sets, sorted sets, streams, geo, HyperLogLog and scripting; see the command reference pages.
- **Pub/Sub.** Subscriptions are exposed as ZIO streams; see [Pub/Sub](./pub-sub.md).
- **Pipelining.** `AsyncRedis` sends commands without waiting for replies; see [AsyncRedis](./async-redis.md).
- **Cluster support.** `Redis.cluster` follows slot redirections; see [Cluster](./cluster.md).
- **Embedded server for tests.** See [Testing](./testing.md).

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

## Resources

- [ZIO Redis](https://www.youtube.com/watch?v=yqFt3b3RBkI) by Dejan Mijic — Redis is one of the most commonly used
  in-memory data structure stores. In this talk, Dejan will introduce ZIO Redis, a purely functional, strongly typed
  client library backed by ZIO, with excellent performance and extensive support for nearly all of Redis' features. He
  will explain the library design using the bottom-up approach - from communication protocol to public APIs. Finally, he
  will wrap the talk by demonstrating the client's usage and discussing its performance characteristics.
