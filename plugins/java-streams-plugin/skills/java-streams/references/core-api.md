# Core Stream API Reference

Table of contents: [Mental model](#mental-model) · [Creating streams](#creating-streams) · [Intermediate operations](#intermediate-operations) · [Terminal operations](#terminal-operations) · [Primitive streams](#primitive-streams) · [One-shot rule](#one-shot-rule)

## Mental model

A stream is a **recipe**, not a container. Building one (`filter`, `map`, `sorted`, ...) does no work at all — it just chains a description of transformations onto a source. Nothing runs until a **terminal operation** (`collect`, `forEach`, `reduce`, `count`, ...) pulls elements through the pipeline.

Two consequences that explain a lot of confusing behavior:

- **Per-element fusion, not per-stage passes.** The pipeline doesn't run `filter` over every element, then `map` over every element, then `sorted` over every element. Instead each element is pulled through *every* intermediate stage before the next element starts (except for stateful/short-circuiting stages that must buffer — see `sorted`, `distinct`, `limit` below). If you `peek` while chasing a bug, the interleaving you see is stage-by-stage per element, not batch-by-batch.
- **Stages can be skipped entirely.** `anyMatch`, `findFirst`, `limit` are short-circuiting: the pipeline can stop pulling elements as soon as it has an answer, which means later elements never visit earlier `map`/`filter` stages at all. A `peek` that "should" print N times may print fewer if a downstream operation short-circuits — this surprises people who use `peek` for anything beyond quick debugging.

## Creating streams

```java
Stream.of("a", "b", "c");                 // varargs
List.of(1, 2, 3).stream();                // any Collection
Arrays.stream(intArray);                  // arrays, incl. primitive overloads -> IntStream
Stream.empty();
Stream.ofNullable(maybeNull);             // Java 9 — 0 or 1 elements, no null-check boilerplate
Stream.concat(streamA, streamB);          // only two at a time; concat in a loop is O(n^2)-ish, prefer Stream.of(a,b,c).flatMap(identity())

// Infinite streams — MUST be bounded downstream (limit/takeWhile) or they hang/OOM
Stream.generate(Math::random);                          // unbounded, unordered
Stream.iterate(1, n -> n * 2);                           // unbounded, ordered: 1,2,4,8,...
Stream.iterate(1, n -> n < 100, n -> n * 2);              // Java 9 — bounded by predicate, no limit() needed, and it's lazy (see advanced-scenarios.md #4)

Files.lines(path);                        // Stream<String>, must be closed (try-with-resources) — backed by a file handle
Pattern.compile(",").splitAsStream(line); // Stream<String>
"a\nb\nc".lines();                        // Java 11 — Stream<String>, no manual splitting on \r\n vs \n
```

`Stream.iterate`/`generate` and `Files.lines` all wrap an external resource or unbounded generator — treat "did I bound this?" and "does this need closing?" as the two questions to ask every time you see one.

## Intermediate operations

All return a new stream and are lazy. Grouped by whether they need to see the *whole* stream before producing output (**stateful**) or can process one element at a time (**stateless**):

**Stateless** (cheap, safe under parallel, order of interleaving doesn't matter):
- `filter(Predicate)`
- `map(Function)` / `mapToInt`/`mapToLong`/`mapToDouble` (widen to primitive stream) / `mapToObj` (narrow back)
- `flatMap(Function<T, Stream<R>>)` — replaces each element with a stream, then flattens one level. See `advanced-scenarios.md` for nested/recursive flattening.
- `mapMulti(BiConsumer<T, Consumer<R>>)` — Java 16. Push zero or more results per input by calling the consumer, instead of allocating an inner `Stream`. See `collectors.md` for when this beats `flatMap`.

**Stateful** (must buffer some or all elements — costly on large/parallel/infinite streams, discussed per-op below):
- `distinct()` — needs to remember every element seen so far (via `equals`/`hashCode`). On an infinite stream this never completes unless every element is a duplicate of an earlier one.
- `sorted()` / `sorted(Comparator)` — must consume the **entire upstream** before emitting anything. Calling `sorted()` on an infinite stream hangs forever. This is the #1 cause of "my stream never finishes."
- `limit(n)` — stateful in a parallel context (has to track how many elements have passed to enforce encounter order), cheap sequentially.
- `skip(n)`
- `takeWhile(Predicate)` / `dropWhile(Predicate)` — Java 9. `takeWhile` short-circuits (stops at first non-match); **`dropWhile` does not** — it still visits every dropped element to test the predicate, it just discards them. Neither is the same as `filter`: both are order-sensitive and only look at a contiguous prefix.
- `peek(Consumer)` — for debugging only. Its actions are explicitly *unordered and may be skipped* per the `Stream` javadoc if the JIT/pipeline determines the result doesn't need them (e.g. upstream of a short-circuiting terminal op). Never rely on `peek` to mutate external state that affects program correctness — see `advanced-scenarios.md` #2.

## Terminal operations

Trigger execution; the stream is consumed afterward (see [One-shot rule](#one-shot-rule)).

| Operation | Notes |
|---|---|
| `forEach(Consumer)` | Order **not** guaranteed on parallel streams. Use `forEachOrdered` if source order matters and you're parallel. |
| `forEachOrdered(Consumer)` | Preserves encounter order even in parallel — but forces the parallel pipeline to synchronize, which can erase the speedup you were parallelizing for. |
| `toArray()` / `toArray(IntFunction<A[]>)` | Prefer `toArray(String[]::new)` (works since Java 11 without an explicit lambda-to-generator dance) over the raw `Object[]` form. |
| `reduce(BinaryOperator)` → `Optional<T>` | No identity; empty stream → empty `Optional`. |
| `reduce(identity, BinaryOperator)` → `T` | Has identity, no `Optional`. Identity must be a true identity for the operator (e.g. `0` for `+`) or parallel results will be wrong. |
| `reduce(identity, accumulator, combiner)` → `U` | The 3-arg form exists *specifically* for parallel execution when accumulating into a different type than the elements — see `advanced-scenarios.md` #15 for why skipping the combiner silently breaks in parallel. |
| `collect(Collector)` | The general-purpose mutable-reduction terminal op — see `collectors.md`. |
| `collect(supplier, accumulator, combiner)` | The 3-arg unpackaged form of the above; rarely needed once you're comfortable with `Collectors`. |
| `min`/`max(Comparator)` → `Optional<T>` | |
| `count()` → `long` | On some pipelines (no filtering, `SIZED` source) the JDK can answer this without visiting elements at all. |
| `anyMatch`/`allMatch`/`noneMatch(Predicate)` | Short-circuiting. `allMatch`/`noneMatch` on an **empty** stream both return `true` (vacuous truth) — a frequent source of off-by-one logic bugs. |
| `findFirst()` → `Optional<T>` | Deterministic even in parallel, but may force extra synchronization to establish "first." |
| `findAny()` → `Optional<T>` | Not deterministic in parallel — allowed to return whichever element a worker thread finishes first. Use it instead of `findFirst` when you only need *an* answer and don't care which, for better parallel throughput. |
| `toList()` | Java 16 — shorthand for `collect(Collectors.toUnmodifiableList())`. **Note the result is unmodifiable**, unlike `Collectors.toList()` which returns a mutable `ArrayList` with no guarantee about mutability. Don't assume `.toList()` results can be `.add()`ed to. |

## Primitive streams

`IntStream`, `LongStream`, `DoubleStream` exist to avoid boxing every element into `Integer`/`Long`/`Double`. Prefer them whenever you're doing numeric aggregation:

```java
// Boxed — allocates an Integer per element, reduce() does unboxing/reboxing per step
int total = list.stream().map(Order::amount).reduce(0, Integer::sum);

// Primitive — no boxing anywhere in the pipeline
int total = list.stream().mapToInt(Order::amount).sum();
```

Extra terminal ops only primitive streams have: `sum()`, `average()` → `OptionalDouble`, `summaryStatistics()` → `IntSummaryStatistics`/etc. (min, max, sum, average, count in one pass — see `collectors.md`'s `teeing` section for the generic version of this idea over arbitrary types).

Bridge back to objects with `.boxed()` (→ `Stream<Integer>`) or `.mapToObj(fn)`.

`IntStream.range(0, n)` / `.rangeClosed(0, n)` are the idiomatic replacement for classic index-based `for` loops when you need the index itself inside a stream pipeline (e.g. zipping two lists by position — streams have no built-in `zip`).

## One-shot rule

A stream is consumed by its terminal operation and cannot be reused:

```java
Stream<String> s = list.stream();
s.forEach(System.out::println);
s.count(); // throws IllegalStateException: stream has already been operated upon or closed
```

If you need to run a pipeline multiple times, re-derive the stream from the source each time (e.g. wrap the pipeline in a `Supplier<Stream<T>>` and call `.get()` per use), or collect once into a `List` and stream that.
