---
id: index
title: "Introduction to ZIO SBT"
sidebar_label: "Introduction"
---

ZIO Redis is a type-safe, performant, ZIO native Redis client.

## Installation

Include ZIO Redis in your project by adding the following to your build.sbt file:

```scala mdoc:passthrough
println(s"""```""")
if (zio.redis.BuildInfo.isSnapshot)
  println(s"""resolvers += Resolver.sonatypeRepo("snapshots")""")
println(s"""libraryDependencies += "dev.zio" %% "zio-redis" % "${zio.redis.BuildInfo.version}"""")
println(s"""```""")
```
