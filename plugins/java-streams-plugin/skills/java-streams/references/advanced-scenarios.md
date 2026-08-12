# Advanced & Difficult Scenarios

Worked examples of the stream mistakes and techniques that don't show up in introductory material. Each entry: the broken/naive version, why it's wrong, and the fix. Use these as templates — the specific domain objects (`Order`, `Person`) are stand-ins, the pattern is what transfers.

Table of contents: [1. Stateful lambdas + parallel](#1-stateful-lambdas-in-parallel-streams) · [2. peek() abuse](#2-relying-on-peek-for-side-effects) · [3. sorted()/distinct() on infinite streams](#3-sorteddistinct-on-infinite-streams) · [4. Bounded iterate](#4-streamiterate-with-a-predicate-vs-limit) · [5. Nested flatMap](#5-flatmap-over-nested--recursive-structures) · [6. Checked exceptions](#6-checked-exceptions-inside-lambdas) · [7. toMap duplicate keys](#7-toMap-duplicate-key-explosion) · [8. Word frequency + tie-break sort](#8-word-frequency-with-stable-tie-breaking) · [9. Multi-key grouping](#9-multi-key-grouping-and-reshaping) · [10. teeing vs two passes](#10-teeing-to-avoid-two-passes) · [11. Parallel ordering](#11-parallel-ordering-and-source-choice) · [12. Custom Spliterator](#12-custom-spliterator-for-an-unsplittable-source) · [13. reduce's 3-arg trap](#13-reduces-3-arg-combiner-is-not-optional-in-parallel) · [14. Boxing cost](#14-boxing-cost-in-numeric-reduction) · [15. Record deconstruction](#15-record-pattern-deconstruction-in-pipelines-java-21)

## 1. Stateful lambdas in parallel streams

```java
// BROKEN — race condition, and even sequentially it's an anti-pattern
List<String> results = new ArrayList<>();
items.parallelStream().forEach(item -> results.add(transform(item))); // ArrayList isn't thread-safe: lost updates, ArrayIndexOutOfBounds, or infinite loop under contention
```
`forEach` gives no guarantee about which thread touches which element, so mutating a shared, non-thread-safe collection from inside it is a data race — and it's silent under light load, so it can pass tests and fail in production under real concurrency.

```java
// FIXED — let collect() own the accumulation, it's built for this
List<String> results = items.parallelStream()
    .map(this::transform)
    .collect(Collectors.toList()); // combiner-safe merge of thread-local partial results
```
General rule: if you find yourself declaring a mutable variable *outside* a stream pipeline and mutating it *inside* `forEach`/`map`/`peek`, that's almost always a sign the logic belongs in `collect`/`reduce` instead, parallel or not — even sequential code with this shape is fragile to future parallelization and non-obvious about ordering guarantees.

## 2. Relying on peek() for side effects

```java
// BROKEN — may not run for every element, or at all, depending on the rest of the pipeline
List<String> names = people.stream()
    .peek(p -> auditLog.add(p.name()))   // "record everyone we processed"
    .filter(Person::isActive)
    .map(Person::name)
    .findFirst()                          // short-circuits! peek may run 1 time, not N times
    .stream().toList();
```
The `Stream` javadoc explicitly reserves the right to skip or reorder `peek` actions when the pipeline can determine they don't affect the result — this isn't a corner-case bug, it's documented behavior that trips people up because `peek` *looks* like `forEach` inserted mid-chain.

```java
// FIXED — do the side effect in a real terminal op, or don't stream for it at all
List<Person> active = people.stream().filter(Person::isActive).toList();
active.forEach(p -> auditLog.add(p.name()));
Optional<String> firstName = active.stream().map(Person::name).findFirst();
```
`peek` is fine for one-off `System.out` debugging you'll delete before committing. Treat anything that touches program state as disqualifying.

## 3. sorted()/distinct() on infinite streams

```java
// HANGS FOREVER — sorted() must exhaust the upstream before it can emit element #1
Stream.iterate(1, n -> n + 1).sorted().findFirst();
```
`distinct()` has the same problem in the pathological case (an infinite stream of never-repeating values never lets `distinct` conclude an element is safe to emit... actually it can emit eagerly since distinct is stateless-per-element-forward, but bounding is still required for termination if the *source* doesn't terminate). The rule to hand someone: **any stateful, whole-stream-buffering operation (`sorted`, or `distinct`/`collect` against an unbounded source) needs a bound (`limit`, `takeWhile`) upstream of it, not downstream.**

```java
// FIXED — bound before sorting
Stream.iterate(1, n -> n + 1).limit(1000).sorted().toList();
```

## 4. Stream.iterate with a predicate vs limit()

```java
// Works, but couples the generation logic to an unrelated element count
Stream.iterate(1, n -> n * 2).limit(10).takeWhile(n -> n < 1000).toList();

// Cleaner (Java 9+) — the bound *is* the stopping condition, no guessing an element count
Stream.iterate(1, n -> n < 1000, n -> n * 2).toList();
```
Beyond style, the 3-arg form is strictly lazier: the 2-arg `iterate` + `limit` still generates exactly `limit` elements even if `takeWhile` would've stopped earlier, whereas the 3-arg form's predicate is checked *before* each element is produced.

## 5. flatMap over nested / recursive structures

Flattening a fixed depth (`List<List<T>>`) is `flatMap(List::stream)`. Arbitrary/recursive depth (a tree, a filesystem, nested categories) needs `flatMap` to call back into itself:

```java
record Category(String name, List<Category> children, List<Product> products) {}

// Recursively stream every product in a category tree, regardless of nesting depth
static Stream<Product> allProducts(Category c) {
    return Stream.concat(
        c.products().stream(),
        c.children().stream().flatMap(Category::allProducts)   // recursive flatMap
    );
}
```
This is genuinely recursive — each call to `allProducts` returns a lazy `Stream` that, when pulled, recursively pulls from child streams. It's a clean way to linearize a tree without writing an explicit stack/queue traversal, but be aware very deep trees pay one stack frame per level of nesting when the stream is finally consumed.

## 6. Checked exceptions inside lambdas

```java
// DOES NOT COMPILE — Function<T,R> can't declare IOException, Files.readString throws it
List<String> contents = paths.stream().map(Files::readString).toList();
```
Functional interfaces used by `Stream` don't declare checked exceptions, so any lambda/method reference that can throw one won't type-check. Three real options, in order of preference:

```java
// Option A — catch and convert to unchecked at the call site (usually the clearest)
List<String> contents = paths.stream()
    .map(p -> {
        try { return Files.readString(p); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    })
    .toList();

// Option B — a small reusable wrapper if this pattern repeats across a codebase
static <T, R> Function<T, R> unchecked(CheckedFunction<T, R> f) {
    return t -> { try { return f.apply(t); } catch (Exception e) { throw new RuntimeException(e); } };
}
interface CheckedFunction<T, R> { R apply(T t) throws Exception; }
// usage: paths.stream().map(unchecked(Files::readString)).toList();

// Option C — for genuinely recoverable per-element failures, don't let the exception
// propagate through the stream at all; model failure as data instead
record Attempt<T>(T value, Exception error) {}
List<Attempt<String>> attempts = paths.stream()
    .map(p -> { try { return new Attempt<>(Files.readString(p), null); }
                catch (IOException e) { return new Attempt<String>(null, e); } })
    .toList();
```
Option C matters more than it looks: if even one file in a large batch throwing should not abort the whole pipeline, don't let the exception unwind the stream at all — capture it as a value and let the caller decide what "partial failure" means for their use case.

## 7. toMap duplicate-key explosion

Covered in `collectors.md`, worth restating as a scenario: this is the single most common runtime-only stream bug, because it depends on the *data*, not the code, and passes code review and unit tests built on non-colliding sample data:

```java
// Works in dev with clean test data, throws IllegalStateException in prod when two Orders share a customerId
Map<String, Order> latestOrderByCustomer = orders.stream()
    .collect(Collectors.toMap(Order::customerId, Function.identity()));
```
Whenever the key comes from real-world/user-controlled data rather than a known-unique identifier (primary key from a DB query is usually safe; a derived or user-supplied field is not), default to writing the 3-arg `toMap` with an explicit merge policy even if you currently believe the data can't collide.

## 8. Word frequency with stable tie-breaking

A compact example that chains `groupingBy` → `counting` → sort-by-value with a documented tie-break, a combination that comes up constantly (leaderboard, top-N, frequency analysis):

```java
Map<String, Long> freq = Arrays.stream(text.toLowerCase().split("\\W+"))
    .filter(Predicate.not(String::isBlank))
    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

List<Map.Entry<String, Long>> topWords = freq.entrySet().stream()
    .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
            .thenComparing(Map.Entry.comparingByKey()))   // tie-break alphabetically — without this, tie order depends on HashMap's unspecified iteration order and can differ between JVM runs
    .limit(10)
    .toList();
```
The `thenComparing` tie-break isn't decoration — omitting it makes the top-N list's order among tied counts nondeterministic (`HashMap` iteration order isn't specified, and can even differ between JDK versions), which turns into a flaky test or a "why did this report change with no data change" bug report.

## 9. Multi-key grouping and reshaping

```java
// Map<Region, Map<Status, Double>> — total order amount per region, per status
Map<Region, Map<Status, Double>> totalsByRegionAndStatus = orders.stream()
    .collect(Collectors.groupingBy(Order::region,
             Collectors.groupingBy(Order::status,
             Collectors.summingDouble(Order::amount))));

// Flattening that same shape into a flat list of rows, e.g. for a CSV export
record RegionStatusTotal(Region region, Status status, double total) {}

List<RegionStatusTotal> rows = totalsByRegionAndStatus.entrySet().stream()
    .flatMap(regionEntry -> regionEntry.getValue().entrySet().stream()
        .map(statusEntry -> new RegionStatusTotal(
            regionEntry.getKey(), statusEntry.getKey(), statusEntry.getValue())))
    .toList();
```
The flatten-a-nested-map-into-rows pattern (`entrySet().stream().flatMap(outer -> inner.entrySet().stream().map(...))`) is worth recognizing on sight — it's the standard way back from a `groupingBy(groupingBy(...))` pivot table shape to a flat row list.

## 10. teeing to avoid two passes

```java
// Naive — iterates the (possibly expensive-to-produce, e.g. from a DB cursor) stream twice
double total = orders.stream().mapToDouble(Order::amount).sum();      // pass 1
long count = orders.stream().count();                                  // pass 2 — if `orders` is a one-shot Stream<Order> from a query, this THROWS (see core-api.md's one-shot rule); if it's a List, it's merely wasteful

// Fixed — one pass, and it also works when the source can only be consumed once
record OrderStats(double total, long count) {}
OrderStats stats = orders.stream().collect(Collectors.teeing(
    Collectors.summingDouble(Order::amount),
    Collectors.counting(),
    OrderStats::new));
```
Beyond the performance angle, this fixes a correctness bug that's easy to miss: if `orders` is itself a `Stream` (not a re-streamable `List`/`Collection` — e.g. it came from `Files.lines` or a JDBC result-set adapter), the naive two-pass version throws `IllegalStateException` on the second pass, full stop.

## 11. Parallel ordering and source choice

```java
// Nondeterministic print order — expected for parallel forEach, surprises people the first time
list.parallelStream().forEach(System.out::println);

// Deterministic, but re-synchronizes the pipeline to restore order — measure whether you actually kept your parallel speedup
list.parallelStream().forEachOrdered(System.out::println);
```
Separately, **not every source parallelizes equally well** — parallel speed depends on how cheaply the `Spliterator` can split the source into independent chunks:
- `ArrayList`, arrays, `IntStream.range` — split in O(1), roughly equal-sized chunks. Good parallel candidates.
- `HashSet`/`HashMap` — splits reasonably along hash-table buckets. Decent.
- `LinkedList` — has to walk node-by-node to find a split point, meaning splitting itself is O(n). Parallelizing a `LinkedList` stream frequently makes things *slower*, not faster.
- Anything wrapping an `Iterator` (including most "generate a stream from an external system" adapters) — often can't be split at all, so `.parallel()` silently does nothing useful.

Before reaching for `.parallel()`, the two questions worth asking are "is the source an `ArrayList`/array/range, or something that splits cheaply?" and "is the per-element work CPU-bound and non-trivial (not just addition)?" If either answer is no, measure before assuming parallel helps — see `parallel-and-performance.md`.

## 12. Custom Spliterator for an unsplittable source

When you have a genuinely custom sequential source (e.g. reading fixed-size records from a binary format) and want it to participate properly in `parallel()` or just want fine control over its reported characteristics, implement `Spliterator` directly rather than adapting an `Iterator` (iterator-derived spliterators can't split and lose size/ordering hints):

```java
class FixedRecordSpliterator implements Spliterator<byte[]> {
    private final ByteBuffer buffer;
    private final int recordSize;
    FixedRecordSpliterator(ByteBuffer buffer, int recordSize) {
        this.buffer = buffer; this.recordSize = recordSize;
    }
    public boolean tryAdvance(Consumer<? super byte[]> action) {
        if (buffer.remaining() < recordSize) return false;
        byte[] record = new byte[recordSize];
        buffer.get(record);
        action.accept(record);
        return true;
    }
    public Spliterator<byte[]> trySplit() {
        int remainingRecords = buffer.remaining() / recordSize;
        if (remainingRecords < 2) return null;                      // too small to split further
        int half = (remainingRecords / 2) * recordSize;
        ByteBuffer prefix = buffer.slice(buffer.position(), half);
        buffer.position(buffer.position() + half);                  // this stream keeps the second half
        return new FixedRecordSpliterator(prefix, recordSize);       // returned spliterator owns the first half
    }
    public long estimateSize() { return buffer.remaining() / recordSize; }
    public int characteristics() { return ORDERED | SIZED | SUBSIZED | IMMUTABLE; }
}

// StreamSupport.stream(spliterator, parallel) is the bridge from a Spliterator to a Stream
Stream<byte[]> records = StreamSupport.stream(
    new FixedRecordSpliterator(buffer, recordSize), true);
```
Declaring accurate `characteristics()` (`SIZED`/`SUBSIZED` in particular) is what lets the parallel framework balance work across threads efficiently instead of falling back to conservative, expensive splitting heuristics — an honest `estimateSize()` and characteristics flags matter as much as `trySplit` correctness for actual parallel performance.

## 13. reduce's 3-arg combiner is not optional in parallel

```java
// Works sequentially, WRONG in parallel: the identity+accumulator alone can't merge partial results from different threads
int totalLength = strings.parallelStream()
    .reduce(0, (partialSum, s) -> partialSum + s.length(), Integer::sum);
// ^ this one is actually fine because the combiner IS supplied — contrast with:

List<String> broken = strings.parallelStream()
    .reduce(new ArrayList<String>(),
            (list, s) -> { list.add(s); return list; },   // mutates and returns the SAME list
            (list1, list2) -> { list1.addAll(list2); return list1; });
// compiles and often "looks" like it works on small inputs, but mutating a shared-reference
// accumulator that different threads may be handed is a race — this is precisely what
// collect()'s contract (fresh container per thread via the *supplier*, THEN combine) exists to prevent
```
The fix isn't a smarter `reduce` call — it's recognizing that **mutable accumulation belongs to `collect`, not `reduce`.** `reduce` is for combining *immutable* values (numbers, immutable records) where "combine A and B" never mutates A. The moment your accumulator function mutates and returns the same container, switch to `collect(supplier, accumulator, combiner)` or a `Collectors` call, both of which guarantee each thread gets its own fresh container from the *supplier* before any accumulation happens:

```java
List<String> correct = strings.parallelStream().collect(Collectors.toList());
```

## 14. Boxing cost in numeric reduction

```java
// Boxes every element into an Integer, and reduce() boxes/unboxes on every combine step
int total = orders.stream().map(Order::amount).reduce(0, Integer::sum);

// No boxing anywhere — mapToInt produces a primitive IntStream, sum() is a primitive terminal op
int total = orders.stream().mapToInt(Order::amount).sum();
```
This isn't a micro-optimization footnote — on large collections the boxed version allocates one `Integer` wrapper object per element (plus intermediate sums), which shows up directly in GC pressure. Any time a stream's terminal goal is a numeric aggregate (`sum`, `average`, `min`, `max`, `count` combined with a numeric field), route through `mapToInt`/`mapToLong`/`mapToDouble` rather than `map` + boxed `reduce`.

## 15. Record pattern deconstruction in pipelines (Java 21)

```java
sealed interface Event permits Click, Purchase, PageView {}
record Click(String userId, String elementId) implements Event {}
record Purchase(String userId, String sku, double amount) implements Event {}
record PageView(String userId, String url) implements Event {}

// Extract revenue from a mixed event stream — exhaustive, no instanceof/cast boilerplate
double revenue = events.stream()
    .mapToDouble(e -> switch (e) {
        case Purchase(var userId, var sku, var amount) -> amount;
        case Click c -> 0.0;
        case PageView p -> 0.0;
    })
    .sum();
```
Because `Event` is `sealed`, the compiler verifies every permitted subtype is handled — add a fourth event type later and every such `switch` across the codebase fails to compile until updated, which is a meaningfully stronger guarantee than the pre-21 alternative (`instanceof` chains with a `default` that silently swallows new cases at runtime).
