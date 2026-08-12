# Parallel Streams and Performance

Table of contents: [When parallel helps vs hurts](#when-parallel-helps-vs-hurts) · [The shared common pool](#the-shared-common-pool) · [Virtual threads are not parallel streams](#virtual-threads-are-not-parallel-streams) · [Spliterator source quality](#spliterator-source-quality) · [Measuring, not guessing](#measuring-not-guessing)

## When parallel helps vs hurts

`.parallelStream()` / `.parallel()` is a request, and the runtime is free to run it sequentially anyway if the pipeline is short. It genuinely helps when **all** of these hold:

- The source is large enough that per-thread overhead (splitting, task scheduling, merging) is small relative to total work — thousands of elements at minimum, often more; for small collections sequential nearly always wins.
- Per-element work is CPU-bound and non-trivial (real computation, not `x -> x + 1`).
- The source splits cheaply and evenly (see [Spliterator source quality](#spliterator-source-quality)).
- Operations are stateless and associative — no shared mutable state, no order-dependent logic besides what `forEachOrdered`/`collect` already handle correctly.
- Nothing in the pipeline blocks (network calls, file I/O, `synchronized`, waiting on a lock) — see the common-pool section below for why this is especially costly.

It hurts (or at best does nothing) when the source is small, is `LinkedList`/`Iterator`-backed, involves I/O, needs strict ordering (`forEachOrdered` reintroduces the synchronization cost you were trying to avoid), or involves boxing-heavy numeric work that a primitive stream would've avoided sequentially anyway.

## The shared common pool

Every parallel stream in the JVM — across your entire application, not just your code — shares one `ForkJoinPool.commonPool()`, sized by default to `Runtime.availableProcessors() - 1`. Two consequences that matter in real systems, not just theory:

- **Unrelated parallel streams contend with each other.** If a web request handler runs a parallel stream while a background job also runs one, they're queuing on the same fixed-size pool. A slow parallel stream in one code path can starve an unrelated one elsewhere in the same JVM.
- **A blocking call inside a parallel stream ties up a common-pool worker thread for the duration of the block**, shrinking the effective pool for every other parallel stream running anywhere in the process at that moment. This is the concrete mechanism behind "don't do I/O inside a parallel stream" — it's not just slow for you, it degrades throughput application-wide.

If you need true isolation (a dedicated pool sized for a specific workload), the pattern is submitting the stream's terminal operation into a custom `ForkJoinPool` via `.submit(() -> stream...).get()`. Treat this as a specialized, last-resort technique — it relies on an implementation quirk of how `parallelStream()` looks up its pool (documented as working, but easy to get subtly wrong), and for most I/O-bound fan-out needs there's now a better-designed tool: `StructuredTaskScope` (finalized in Java 25, see `lts-updates.md`) or a plain virtual-thread-per-task `ExecutorService`, neither of which touch the stream common pool at all.

## Virtual threads are not parallel streams

Restated from `lts-updates.md` because it's the most common wrong assumption people bring to this topic: **`.parallel()` does not run on virtual threads**, in any current JDK version. Parallel streams are, and remain, backed by the platform-thread `ForkJoinPool.commonPool()`. Virtual threads solve a different problem (cheaply blocking on I/O at massive concurrency); parallel streams solve a different one (splitting one CPU-bound computation across cores). If the actual goal is "run 10,000 concurrent HTTP calls," reach for a virtual-thread executor or structured concurrency, not `.parallelStream()` — putting blocking I/O on the common pool, as above, actively hurts unrelated code elsewhere in the process.

## Spliterator source quality

Parallel performance is bounded by how cheaply and evenly the source's `Spliterator` can divide the work:

| Source | Splits | Why |
|---|---|---|
| `ArrayList`, array, `IntStream.range` | O(1), even | Random access by index — split point is just an index midpoint. |
| `HashSet`/`HashMap` | Reasonable | Splits along internal hash-table structure. |
| `TreeSet`/`TreeMap` | Reasonable | Splits along tree structure, but rebalancing costs make it worse than a hash-based source. |
| `LinkedList` | Poor — O(n) to find a split point | Has to walk node-by-node; splitting can cost as much as the work it's meant to parallelize. |
| `Iterator`-derived (`Stream.generate`, `BufferedReader.lines()` on some implementations, most custom "wrap an external system" adapters) | Often none | No random access means `trySplit()` frequently returns `null` — `.parallel()` silently runs sequentially. |

If you're not sure whether a source parallelizes well, check `spliterator().characteristics()` (`SIZED` and `SUBSIZED` are the strong positive signals) rather than assuming — see `advanced-scenarios.md` #12 for what supplying those characteristics correctly looks like when writing a custom source.

## Measuring, not guessing

Stream parallelism is one of the areas of Java where intuition is wrong often enough that "measure it" isn't a platitude — it's the actual process:

- JIT warmup means a quick `System.currentTimeMillis()` wrapped around one run is close to meaningless; the JIT hasn't compiled hot paths yet on a cold JVM.
- For anything going into a real decision (not just curiosity), use JMH (Java Microbenchmark Harness) rather than hand-rolled timing — it handles warmup iterations, avoids dead-code elimination artifacts, and reports variance.
- For a quick sanity check that doesn't need JMH rigor, run the pipeline many times in a loop *within the same JVM invocation* and discard the first several iterations before timing, to at least get past the worst of JIT warmup — but treat the result as directional, not a benchmark result to put in a PR description.
- When in doubt between sequential and parallel for a pipeline that isn't obviously in the "clearly large + clearly CPU-bound" zone, default to sequential. Sequential is easier to reason about, debug, and won't contend with the rest of the application's use of the shared common pool — parallel is an opt-in optimization for a demonstrated bottleneck, not a default.
