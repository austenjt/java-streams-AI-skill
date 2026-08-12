---
name: java-streams
description: Comprehensive knowledge for writing, reviewing, debugging, and modernizing Java Stream API code — stream creation, intermediate/terminal operations, the full Collectors toolkit (groupingBy composition, teeing, custom collectors), parallel-stream pitfalls, and everything added through Java 25 LTS (Stream.toList(), mapMulti, sequenced collections' reversed(), record-pattern deconstruction in pipelines, structured concurrency vs parallel streams). Use this whenever the user is writing, reviewing, or debugging Java code that touches java.util.stream, asks about Collectors/groupingBy/partitioningBy/teeing, mentions parallel streams or performance of stream code, wants to upgrade/modernize old Java 8-style stream code to a newer LTS, or describes a task that streams naturally solve even without saying "stream" explicitly — e.g. "aggregate this list by category," "dedupe and sort these records," "why is my parallel stream slower / giving wrong results," "flatten this nested list," "group these orders by customer and sum the totals."
---

# Java Streams

Java's `Stream` API rewards precision more than most of the language: small choices (which `Collectors` overload, `map` vs `mapMulti`, `reduce` vs `collect`) are the difference between code that's correct-but-slow, subtly-wrong-under-parallel, or throws in production on data your test fixtures never exercised. This skill exists to get those choices right on the first pass instead of after a bug report.

## How to use this skill

Four reference files hold the depth; read the one(s) relevant to the task at hand rather than loading all of them up front:

- **[references/core-api.md](references/core-api.md)** — stream creation, every intermediate/terminal operation, the laziness/fusion mental model, primitive streams, the one-shot rule. Start here for "how does X operation actually behave."
- **[references/collectors.md](references/collectors.md)** — the full `Collectors` toolkit: `toMap`'s duplicate-key trap, `groupingBy`/`partitioningBy` and composing them with downstream collectors, `teeing`, `collectingAndThen`, writing a custom `Collector.of(...)`, `mapMulti` vs `flatMap`. Read this before writing any grouping/aggregation code.
- **[references/lts-updates.md](references/lts-updates.md)** — what shipped in each JDK version from 9 through 25 LTS, organized so you can answer "is this available on the version this project targets." Read this when modernizing old code or when the user asks what's new.
- **[references/advanced-scenarios.md](references/advanced-scenarios.md)** — 15 worked hard-mode scenarios (stateful-lambda races, `peek` abuse, infinite-stream hangs, checked exceptions in lambdas, custom `Spliterator`, `reduce`'s combiner trap, record-pattern deconstruction, and more), each with a broken version, why it's broken, and the fix. Read this when the task is non-routine or when reviewing someone else's stream code for correctness.
- **[references/parallel-and-performance.md](references/parallel-and-performance.md)** — when `.parallel()` actually helps, why it shares one JVM-wide thread pool, why virtual threads don't power it, and what source types parallelize well. Read this before recommending or reviewing `.parallelStream()`.

If the task is a quick, unambiguous one-liner (e.g. "group this list by field X"), you often don't need to open a reference file at all — the patterns below cover the common cases directly.

## Core decision guide

Work through these questions roughly in order; each one rules out a family of mistakes covered in detail in the reference files.

1. **What's the terminal shape?** A single value (`reduce`/primitive terminal), a `List`/`Set` (`toList()` or `Collectors.toX`), a `Map` (`groupingBy` or `toMap` — check whether keys can collide before picking `toMap`), or a side effect (`forEach`, and ask whether order matters).
2. **Is any aggregate numeric?** If so, route through `mapToInt`/`mapToLong`/`mapToDouble` rather than `map` + boxed `reduce` — see `core-api.md`'s primitive-streams section and `advanced-scenarios.md` #14.
3. **Does the pipeline need more than one thing out of the same pass** (count + sum, min + max, two different groupings)? Reach for `Collectors.teeing` or a composed `groupingBy` downstream before writing two separate passes or a custom loop — see `collectors.md`.
4. **Could any lambda throw a checked exception, or does a key/grouping value potentially collide?** These are the two most common "compiles fine, breaks on real data" traps — see `advanced-scenarios.md` #6 and #7.
5. **Is the source unbounded (`iterate`/`generate`) or does the pipeline include `sorted`/`distinct`?** Confirm there's a `limit`/`takeWhile`/predicate bound *before* the stateful operation, not just somewhere in the chain — see `advanced-scenarios.md` #3.
6. **Is `.parallel()` on the table?** Don't add it reflexively. Confirm source size, source spliterator quality, and that nothing in the pipeline blocks or mutates shared state — see `parallel-and-performance.md`. And never justify `.parallel()` as "to use virtual threads" — it doesn't.
7. **What Java version does this project target?** If modernizing, check `lts-updates.md` before suggesting a method — `toList()`, `mapMulti`, and `teeing` are easy to reach for out of habit even on a codebase still targeting Java 11.

## Quick reference: the calls people reach for most

```java
// Group + count
Map<K, Long> counts = list.stream().collect(Collectors.groupingBy(Thing::key, Collectors.counting()));

// Group + sum (numeric downstream, no boxing)
Map<K, Integer> totals = list.stream().collect(Collectors.groupingBy(Thing::key, Collectors.summingInt(Thing::amount)));

// Group, keep only some fields per group
Map<K, List<V>> names = list.stream().collect(Collectors.groupingBy(Thing::key, Collectors.mapping(Thing::name, Collectors.toList())));

// Safe map-from-list (duplicate keys handled explicitly instead of throwing)
Map<K, V> byKey = list.stream().collect(Collectors.toMap(Thing::key, Function.identity(), (a, b) -> a));

// Two aggregates, one pass
record Stats(long count, double avg) {}
Stats stats = list.stream().collect(Collectors.teeing(Collectors.counting(), Collectors.averagingDouble(Thing::amount), Stats::new));

// Flatten a nested collection
List<Item> flat = list.stream().flatMap(Thing::items).toList();

// Modern shorthand terminal (Java 16+, result is UNMODIFIABLE — see core-api.md)
List<R> mapped = list.stream().map(Thing::transform).toList();
```

## When reviewing someone else's stream code

Scan specifically for the patterns in `advanced-scenarios.md`: a mutable variable declared outside the pipeline and written to inside `forEach`/`peek`; `toMap` without a merge function on data that isn't a guaranteed-unique key; `sorted()`/`distinct()`/`collect()` on a `Stream.iterate`/`generate` source without a visible bound upstream; `reduce` with a mutating accumulator on a `parallelStream()`; and `.parallel()` added without evidence it was measured to help. These five account for the overwhelming majority of real stream bugs.

## A note on staying current

The `Stream`/`Collectors` interfaces themselves have been stable since Java 16 — `lts-updates.md` says this explicitly so you don't invent JDK-version claims. What keeps evolving is the language around streams (records, pattern matching, sequenced collections) and JDK-internal concurrency (virtual threads, structured concurrency), which change how code *around* a pipeline looks without changing the pipeline mechanics. If asked about a JDK feature released after this skill's own knowledge, say so rather than guessing at method signatures — a plausible-sounding but wrong `Collectors` overload is worse than admitting uncertainty.
