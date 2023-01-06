---
id: index
title: "Introduction to ZIO Redis"
sidebar_label: "ZIO Redis"
---

[ZIO Redis](https://github.com/zio/zio-redis) is a ZIO native Redis client.

> The client is still a work-in-progress. Watch this space!

@PROJECT_BADGES@

## Introduction

ZIO Redis is in the experimental phase of development, but its goals are:

- **Type Safety**
- **Performance**
- **Minimum Dependency**
- **ZIO Native**

## Installation

Since the ZIO Redis is in the experimental phase, it is not released yet, but we can use snapshots:

```scala
libraryDependencies += "dev.zio" %% "zio-redis" % "@VERSION@"
```

## Example

To execute our ZIO Redis effect, we should provide the `RedisExecutor` layer to that effect. To create this layer we
should also provide the following layers:

- **RedisConfig** — Using default one, will connect to the `localhost:6379` Redis instance.
- **BinaryCodec** — In this example, we are going to use the built-in `ProtobufCodec` codec from zio-schema project.

To run this example we should put following dependencies in our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-redis" % "@VERSION@",
  "dev.zio" %% "zio-schema-protobuf" % "0.3.0"
)
```

```scala mdoc:compile-only
import zio._
import zio.redis._
import zio.schema.codec._

object ZIORedisExample extends ZIOAppDefault {
  val myApp: ZIO[Redis, RedisError, Unit] = for {
    _ <- set("myKey", 8L, Some(1.minutes))
    v <- get("myKey").returning[Long]
    _ <- Console.printLine(s"Value of myKey: $v").orDie
    _ <- hSet("myHash", ("k1", 6), ("k2", 2))
    _ <- rPush("myList", 1, 2, 3, 4)
    _ <- sAdd("mySet", "a", "b", "a", "c")
  } yield ()

  override def run = myApp.provide(
    RedisLive.layer,
    RedisExecutor.layer,
    ZLayer.succeed(RedisConfig.Default),
    ZLayer.succeed[BinaryCodec](ProtobufCodec)
  )
}
```

## Resources

- [ZIO Redis](https://www.youtube.com/watch?v=yqFt3b3RBkI) by Dejan Mijic — Redis is one of the most commonly used
  in-memory data structure stores. In this talk, Dejan will introduce ZIO Redis, a purely functional, strongly typed
  client library backed by ZIO, with excellent performance and extensive support for nearly all of Redis' features. He
  will explain the library design using the bottom-up approach - from communication protocol to public APIs. Finally, he
  will wrap the talk by demonstrating the client's usage and discussing its performance characteristics.
