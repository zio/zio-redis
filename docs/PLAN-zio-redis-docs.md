# Plan: ZIO Redis documentation (ZIO-33)

Working file. Delete before the last PR, or keep out of the PR (Milad decides).

## 1. Problem

zio.dev/zio-redis is one page (`docs/index.md`, 121 lines): install, one example, one test example, one video link.
It does not explain connecting, config, codecs, the `returning[A]` pattern, any command group, pub/sub, cluster,
errors, or the async client.

## 2. Goal

A full docs set: getting started, one page per command group, pub/sub, cluster, errors, testing, a tutorial and
how-to guides. Every Scala block compiles under mdoc. No invented commands.

## 3. Findings

- `docs/` is the live folder. CI (`ci.yml`) builds the website, `generateReadme` and `publishToNpm` from it.
  mdoc project `docs` points at `zio-redis-docs/`, which does not exist, so mdoc reads `docs/`.
- `website/docs/` is a stale copy (hardcoded version 1.3.0, plain `scala` blocks, nothing references it).
  Proposal: delete it in PR 1.
- zio.dev pins `@zio.dev/zio-redis` at 1.1.10 in the zio/zio repo, so a version bump there is needed for the site
  to show new pages. Separate repo, separate PR.
- Public API: `Redis` (sync: `IO[RedisError, A]`) and `AsyncRedis` (`UIO[IO[RedisError, A]]`, for pipelining).
  Layers: `Redis.local`, `Redis.singleNode`, `Redis.cluster`; `RedisSubscription.local/singleNode`.
  Return types are chosen with `.returning[A]` (needs a zio-schema `Schema`).
  `CodecSupplier` (built-in `CodecSupplier.utf8`) picks the codec.
- No MULTI/EXEC/WATCH API exists (to double-check in PR 1 research). Transactions page = a short "not supported,
  use Lua scripts" note inside the Scripting page, not its own page.
- Command counts: Strings 30, Keys 26, Hashes 16, Lists 19, Sets 15, SortedSets 46, Streams 32, Geo 8,
  HyperLogLog 3, Scripting 7, Publishing 4, Subscription 8, Connection 7, Cluster 6.

## 4. Page list and sidebar order

All under `docs/`. Types: T tutorial, H how-to, R reference, E explanation.

| # | Page | Type | Based on | Size | PR |
|---|------|------|----------|------|----|
| 1 | `index.md` (rewrite: overview, install, quick example, links) | E | current index, README | ~100 lines | 1 |
| 2 | `getting-started.md` (layers, `returning`, first program) | E/R | `Redis.scala`, `ResultBuilder.scala` | ~150 | 1 |
| 3 | `configuration.md` (`RedisConfig`, `RedisUri`, auth, SSL, queue size) | R | `config.scala`, `RedisUri.scala` | ~120 | 1 |
| 4 | `codecs.md` (`CodecSupplier`, utf8, protobuf, custom types) | R | `CodecSupplier.scala`, redis-it `ProtobufCodecSupplier` | ~120 | 1 |
| 5 | `error-handling.md` (`RedisError` cases) | R | `RedisError.scala` | ~100 | 1 |
| 6 | `async-redis.md` (`AsyncRedis`, pipelining) | E | `GenRedis.scala`, `Redis.scala` | ~100 | 1 |
| 7 | `commands/strings.md` | R | `api/Strings.scala`, `options/Strings.scala` | ~250 | 2 |
| 8 | `commands/keys.md` | R | `api/Keys.scala` | ~250 | 2 |
| 9 | `commands/hashes.md` | R | `api/Hashes.scala` | ~200 | 3 |
| 10 | `commands/lists.md` | R | `api/Lists.scala` | ~220 | 3 |
| 11 | `commands/sets.md` | R | `api/Sets.scala` | ~180 | 3 |
| 12 | `commands/sorted-sets.md` | R | `api/SortedSets.scala` | ~300 | 4 |
| 13 | `commands/geo.md`, `commands/hyperloglog.md` | R | `api/Geo.scala`, `api/HyperLogLog.scala` | ~200 | 4 |
| 14 | `commands/streams.md` | R | `api/Streams.scala` | ~300 | 5 |
| 15 | `commands/scripting.md` (+ no-transactions note) | R | `api/Scripting.scala` | ~150 | 5 |
| 16 | `commands/connection.md` | R | `api/Connection.scala` | ~100 | 5 |
| 17 | `pub-sub.md` (`Publishing`, `Subscription`, `RedisSubscription`) | R/E | `api/Publishing.scala`, `api/Subscription.scala`, `PubSubSpec` | ~200 | 6 |
| 18 | `cluster.md` (`Redis.cluster`, `RedisClusterConfig`, retry, `Cluster` cmds) | R/E | `config.scala`, `api/Cluster.scala`, `ClusterSpec` | ~180 | 6 |
| 19 | `testing.md` (`EmbeddedRedis`) | H | `modules/embedded`, current index | ~100 | 6 |
| 20 | `tutorial-rate-limiter.md` (small rate limiter) | T | Strings/Keys APIs | ~250 | 7 |
| 21 | `how-to-cache-with-ttl.md` | H | Strings/Keys | ~120 | 7 |
| 22 | `how-to-distributed-lock.md` (SET NX PX + Lua release) | H | Strings, Scripting | ~130 | 7 |

Sidebar: Overview, Getting Started, Configuration, Codecs, Error Handling, AsyncRedis, then category "Commands"
(Strings, Keys, Hashes, Lists, Sets, Sorted Sets, Geo, HyperLogLog, Streams, Scripting, Connection), Pub/Sub,
Cluster, Testing, then category "Guides" (tutorial, two how-tos).

## 5. PR split (approved by Milad: ONE PR)

All pages above go into a single PR on branch `redis-docs`, committed page by page. `website/docs/` is deleted in it.
The plan file is removed before the PR is opened. Examples are `mdoc:compile-only`.

## 5b. Publishing note and open questions

- zio.dev is stuck at 1.1.10: npm `@zio.dev/zio-redis` latest is 1.1.10 and the "Release Docs" job failed on the v1.3.0
  and v1.2.1 releases. New docs reach zio.dev only after: merge, a release whose Release Docs job succeeds, then a bump of
  `@zio.dev/zio-redis` in zio/zio `website/package.json`. PR #1329 may have fixed the npm step; untested.
- Open question for Milad: investigate the Release Docs failure (`gh run view <id> -R zio/zio-redis --log-failed`)
  as a separate task? Not part of this PR.

## 6. Method

- Command pages: grouped by purpose, each command name, options and return type checked against `api/*.scala`
  and `options/`. Examples `mdoc:compile-only` (live Redis needed).
- `EmbeddedRedis` used only on the testing page, `mdoc:compile-only`.
- Per PR: `sbt docs/mdoc`, `sbt scalafmtAll`, `sbt "++2.13; check"`, sidebar and link check, style review.
