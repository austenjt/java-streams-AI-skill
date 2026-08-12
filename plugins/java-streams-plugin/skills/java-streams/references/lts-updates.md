# What Changed, By Java Version

The Stream *interfaces* (`Stream`, `IntStream`, `Collector`, ...) have been essentially frozen since Java 16 — no new abstract methods have been added since. What keeps changing is (a) convenience methods bolted onto that stable API, and (b) surrounding language features (records, pattern matching, sequenced collections) that streams increasingly compose with. This file is organized by JDK version so you can quickly answer "is this available on the Java version this codebase targets?" Non-LTS releases are included where they shipped a feature that matters, since features ship once and simply carry forward into the next LTS.

## Java 8 (baseline — for contrast)

`java.util.stream` itself, lambdas, method references, `Optional`, the original `Collectors` set (`toList`, `toSet`, `toMap`, `groupingBy`, `partitioningBy`, `joining`, `summingX`, `averagingX`, `reducing`). Everything below is measured relative to this baseline.

## Java 9

- `takeWhile` / `dropWhile` — see `core-api.md`.
- `Stream.iterate(seed, hasNext, next)` — bounded, lazy generation without a separate `limit`; see `advanced-scenarios.md` #4.
- `Stream.ofNullable(T)` — turns a possibly-null reference into a 0-or-1-element stream, useful inside `flatMap`: `.flatMap(x -> Stream.ofNullable(lookup(x)))`.
- `Collectors.filtering` — see `collectors.md`'s downstream-composition section.
- `Optional.stream()` — turns `Optional<T>` into a 0-or-1-element `Stream<T>`, which is the idiomatic way to `flatMap` away a collection of `Optional`s: `list.stream().flatMap(x -> maybe(x).stream())`.

## Java 10

- `Collectors.toUnmodifiableList/Set/Map` — see `collectors.md`.
- `var` for local variables — mostly a readability tool in long stream chains, no semantic effect on streams themselves.

## Java 11

- `Collection.toArray(IntFunction<T[]>)` — enables `list.toArray(String[]::new)` cleanly.
- `String.lines()` → `Stream<String>` — replaces manual `split("\\r?\\n")`.
- `Predicate.not(...)` — handy inside `filter` for negating a method reference: `.filter(Predicate.not(String::isBlank))`.

## Java 12 (non-LTS, carries into 17)

- `Collectors.teeing` — see `collectors.md`. Small feature, disproportionately useful; don't skip it just because 12 wasn't an LTS release.

## Java 16 (non-LTS, carries into 17)

- `Stream.toList()` — shorthand terminal op; **returns an unmodifiable list**, unlike `Collectors.toList()`. This is the single most common upgrade suggestion when modernizing pre-16 code: replace `.collect(Collectors.toList())` with `.toList()` *only if* callers don't need to mutate the result — check call sites before blindly substituting.
- `Stream.mapMulti` / `mapMultiToInt/Long/Double` — see `collectors.md`.
- Records finalized — pairs naturally with streams as an immutable per-element data carrier (`orders.stream().map(o -> new Summary(o.id(), o.total()))`).

## Java 17 (LTS)

No new methods landed on `Stream`/`Collectors` themselves in 17 — it's an LTS checkpoint that bundles everything through Java 16 above. What *is* new and stream-adjacent:

- Sealed classes + pattern matching for `instanceof` (finalized in 16/17) make `filter(x -> x instanceof Foo)` followed by a cast obsolete in favor of `filter(x -> x instanceof Foo f && f.isValid())`, binding the cast variable inline inside the predicate.
- `RandomGenerator` interface unifies random-number APIs and adds stream-producing methods directly: `RandomGenerator.getDefault().ints(count, origin, bound)` — prefer this over the legacy `new Random().ints(...)` for new code, and definitely over `Stream.generate(Math::random)` when you need a bounded range.

## Java 21 (LTS)

This is where things get genuinely relevant to stream code:

- **Sequenced Collections** (`SequencedCollection`, `SequencedSet`, `SequencedMap`) add `getFirst()`, `getLast()`, `addFirst()`, `addLast()`, and — most relevant here — `reversed()` to `List`, `Deque`, `LinkedHashSet`, `LinkedHashMap`. `list.reversed().stream()` is now the idiomatic way to stream in reverse order without `Collections.reverse` (which mutates) or manual index counting. `LinkedHashMap.sequencedEntrySet()` similarly gives ordered-map iteration a first-class reversed view.
- **Record patterns + pattern matching for `switch`, both finalized** — you can now deconstruct records directly inside a stream lambda instead of chaining accessor calls:
  ```java
  sealed interface Shape permits Circle, Rectangle {}
  record Circle(double radius) implements Shape {}
  record Rectangle(double w, double h) implements Shape {}

  List<Double> areas = shapes.stream()
      .map(s -> switch (s) {
          case Circle(double r) -> Math.PI * r * r;
          case Rectangle(double w, double h) -> w * h;
      })
      .toList();
  ```
  The switch is exhaustive over the sealed hierarchy with no `default` needed — the compiler enforces every `Shape` subtype is handled, which catches missing cases at compile time instead of via an NPE or wrong `map` output at runtime.
- **Virtual threads (finalized)** — the single most common misconception to correct: **parallel streams do not use virtual threads.** `stream.parallel()` still dispatches work to the shared `ForkJoinPool.commonPool()`, backed by *platform* threads, exactly as it did before Java 21. Virtual threads are a tool for *I/O-bound* concurrency (thousands of blocking calls, cheap to park); parallel streams are a tool for *CPU-bound data parallelism* (splitting one computation across cores). Don't reach for `.parallel()` to "get virtual threads" for a stream full of blocking network calls — see `parallel-and-performance.md` for what to use instead.
- **Structured concurrency** (preview in 21, see Java 25 below for finalization) — the actual right tool for "run these N independent I/O-bound tasks concurrently and collect results," which people sometimes reach for `.parallel().map(blockingCall)` to approximate. It isn't a `Stream` API at all (`StructuredTaskScope`), but it's worth knowing it exists so you can point someone at it instead of a parallel stream full of network calls.

## Java 25 (LTS)

As with 17, **no new methods on `Stream`/`Collectors`** — the data-processing API has been stable since Java 16. What matters for anyone working near streams:

- **Structured Concurrency finalized** (was preview in 21) — `StructuredTaskScope` is now a supported API for fanning out concurrent I/O-bound subtasks with a single point of error handling and cancellation. Mentioned here specifically so it doesn't get conflated with — or substituted by — `.parallel()`, which remains the wrong tool for blocking I/O regardless of which JDK you're on.
- **Scoped Values finalized** — a `ThreadLocal` alternative safe to share immutable context across worker threads, relevant if a parallel stream's operations need access to per-request/per-task context without the pitfalls of mutable `ThreadLocal` state leaking across the shared common pool's reused threads.
- Primitive types in patterns, flexible constructor bodies, module import declarations, compact source files — language features finalized around this release, none of which change how `java.util.stream` itself behaves; they mostly make the *code around* stream pipelines (constructors building the objects a stream will later consume, small scripts using streams) more concise.

**Practical upshot when modernizing older code:** the real "streams API" upgrade path mostly stops at Java 16 (`toList()`, `mapMulti`, `teeing`). What continues evolving past that is what streams compose *with* — records, pattern matching, sequenced collections — not the stream pipeline mechanics themselves. If someone asks "what's new for streams in Java 25 vs 21," the honest answer is "nothing in the Stream API directly; what's new is language/runtime features that make the code around your streams nicer."
