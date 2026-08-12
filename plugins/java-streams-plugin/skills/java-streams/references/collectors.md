# Collectors Reference

Table of contents: [Basic collectors](#basic-collectors) · [toMap and its trap](#tomap-and-its-trap) · [groupingBy](#groupingby) · [downstream collector composition](#downstream-collector-composition) · [partitioningBy](#partitioningby) · [teeing](#teeing) · [collectingAndThen](#collectingandthen) · [Custom collectors](#custom-collectors) · [mapMulti vs flatMap](#mapmulti-vs-flatmap)

`collect(Collector)` is a **mutable reduction**: it builds a single mutable result container (list, map, string, custom object) by folding elements into it, optionally combining partial containers in parallel. Every `Collectors.xxx()` factory just wires up the four ingredients a `Collector` needs: a container supplier, an accumulator, a combiner (for parallel merging), and an optional finisher.

## Basic collectors

```java
Collectors.toList();                 // mutable ArrayList, NOT guaranteed unmodifiable/thread-safe/serializable — just "a List"
Collectors.toUnmodifiableList();      // Java 10 — actually immutable; prefer this (or Stream.toList(), Java 16) when the caller shouldn't mutate the result
Collectors.toSet();                   // no order guarantee (typically HashSet)
Collectors.toUnmodifiableSet();       // Java 10
Collectors.joining();                 // no separator
Collectors.joining(", ");             // delimiter
Collectors.joining(", ", "[", "]");   // delimiter, prefix, suffix
Collectors.counting();                // -> Long — usually paired as a downstream collector, see below
Collectors.summingInt/Long/Double(fn);
Collectors.averagingInt/Long/Double(fn);   // -> Double, always — averaging an empty stream gives 0.0, not NaN or empty
Collectors.summarizingInt/Long/Double(fn); // -> IntSummaryStatistics/etc (min, max, sum, average, count together)
Collectors.minBy(comparator);         // -> Optional<T>, downstream form of Stream.min
Collectors.maxBy(comparator);
Collectors.reducing(identity, BinaryOperator);       // Collector form of Stream.reduce — mainly useful as a *downstream* collector (e.g. summing inside groupingBy) where Stream.reduce isn't directly available
```

## toMap and its trap

```java
Collectors.toMap(Person::id, Function.identity());
```

**This throws `IllegalStateException: Duplicate key` at runtime** the moment two elements produce the same key — it's not a compile-time or type concern, so it slips past review and blows up in production on the first duplicate. Always ask "can this key repeat?" before using the 2-arg form. Fix with the 3-arg merge-function overload:

```java
// keep the first, keep the last, or combine
Collectors.toMap(Person::id, Function.identity(), (first, second) -> first);
Collectors.toMap(Order::customerId, Order::amount, Integer::sum);   // classic "sum amounts by customer" one-liner

// 4-arg form also lets you pick the Map implementation, e.g. to preserve encounter order
Collectors.toMap(Person::id, Function.identity(), (a, b) -> a, LinkedHashMap::new);
```

`Collectors.toUnmodifiableMap` has the same 2-arg duplicate-key trap and the same 3-arg fix.

## groupingBy

```java
// 1-arg: Map<K, List<V>>
Map<Status, List<Order>> byStatus = orders.stream().collect(Collectors.groupingBy(Order::status));

// 2-arg: classifier + downstream collector — this is where groupingBy earns its keep
Map<Status, Long> countByStatus =
    orders.stream().collect(Collectors.groupingBy(Order::status, Collectors.counting()));

// 3-arg: classifier + map factory + downstream — control the Map implementation (e.g. EnumMap for enum keys, TreeMap for sorted keys)
Map<Status, Long> sortedCounts =
    orders.stream().collect(Collectors.groupingBy(Order::status, TreeMap::new, Collectors.counting()));
```

Use `Collectors.groupingByConcurrent(...)` instead of `groupingBy` **only** when the upstream is a parallel stream and you want the grouping itself to build the map concurrently rather than merging per-thread partial maps — it returns a `ConcurrentMap` and does *not* preserve encounter order within groups.

## Downstream collector composition

`groupingBy`'s real power is that the downstream collector can be *any* collector, including another `groupingBy` — this is how you build multi-key aggregations without writing a custom reduction:

```java
// group, then transform each group (extract just names instead of full Order objects)
Map<Status, List<String>> namesByStatus = orders.stream()
    .collect(Collectors.groupingBy(Order::status,
             Collectors.mapping(Order::customerName, Collectors.toList())));

// group, then filter within each group (Java 9's Collectors.filtering) —
// NOT the same as .filter().collect(groupingBy(...)), which would drop statuses entirely
// if every order for that status got filtered out. filtering() keeps the key with an empty list.
Map<Status, List<Order>> bigOrdersByStatus = orders.stream()
    .collect(Collectors.groupingBy(Order::status,
             Collectors.filtering(o -> o.amount() > 1000, Collectors.toList())));

// two-level grouping: Map<Status, Map<Region, List<Order>>>
Map<Status, Map<Region, List<Order>>> byStatusThenRegion = orders.stream()
    .collect(Collectors.groupingBy(Order::status,
             Collectors.groupingBy(Order::region)));

// group + sum, in one pass, no manual accumulator needed
Map<Region, Integer> totalByRegion = orders.stream()
    .collect(Collectors.groupingBy(Order::region, Collectors.summingInt(Order::amount)));
```

The pattern to internalize: **classifier decides the keys, downstream decides what happens to each bucket.** Reach for `mapping`/`filtering`/`counting`/`summingX`/nested `groupingBy` before reaching for a hand-rolled loop with a `HashMap` and `computeIfAbsent`.

## partitioningBy

Like `groupingBy` but the classifier is always a `Predicate`, so the result **always has exactly two keys, `true` and `false`, even if one bucket is empty**:

```java
Map<Boolean, List<Order>> partitioned = orders.stream()
    .collect(Collectors.partitioningBy(o -> o.amount() > 1000));
// partitioned.get(false) is present with an empty list if every order was > 1000 — not absent
```

Prefer this over `groupingBy(predicate)` when the split is genuinely binary — it's clearer intent and the always-both-keys guarantee avoids `null`/`getOrDefault` boilerplate downstream.

## teeing

Java 12. Runs **two** downstream collectors over the same stream in a single pass and merges their results with a `BiFunction`. This is the general answer to "I need two different aggregates from one stream without iterating twice or writing a custom collector":

```java
record MinMax(int min, int max) {}

MinMax range = numbers.stream().collect(Collectors.teeing(
    Collectors.minBy(Integer::compareTo),
    Collectors.maxBy(Integer::compareTo),
    (min, max) -> new MinMax(min.orElseThrow(), max.orElseThrow())
));

// average without two passes and without IntSummaryStatistics if you only want these two numbers
record Stats(long count, double average) {}

Stats stats = orders.stream().collect(Collectors.teeing(
    Collectors.counting(),
    Collectors.averagingInt(Order::amount),
    Stats::new
));
```

If you need *more* than two simultaneous aggregates over primitives, `summarizingInt/Long/Double` (count+sum+min+max+average together) is usually simpler than nesting `teeing`.

## collectingAndThen

Post-process a collector's result without a separate statement — most common use is locking down mutability after building with a plain collector:

```java
List<String> names = stream.collect(Collectors.collectingAndThen(
    Collectors.toList(), Collections::unmodifiableList));

// or adapt a collector's output type, e.g. force a specific List implementation
LinkedList<String> asLinkedList = stream.collect(Collectors.collectingAndThen(
    Collectors.toList(), LinkedList::new));
```

## Custom collectors

Reach for `Collector.of(...)` when no combination of the built-ins expresses what you need — e.g. a running data structure that isn't a `List`/`Map`/`String`, or an accumulation with domain-specific merge logic:

```java
// Collect into a custom immutable histogram-like type
public static <T> Collector<T, ?, Map<T, Long>> toFrequencyMap() {
    return Collector.of(
        HashMap::new,                                              // supplier: new mutable container
        (map, item) -> map.merge(item, 1L, Long::sum),             // accumulator: fold one element in
        (map1, map2) -> { map2.forEach((k, v) -> map1.merge(k, v, Long::sum)); return map1; }, // combiner: merge two partial containers (parallel streams)
        Collector.Characteristics.UNORDERED                        // hints (see below)
    );
}
```

The `Characteristics` you can declare, and why they matter:
- `CONCURRENT` — the accumulator is thread-safe and multiple threads can share **one** container instead of building separate partial containers and merging. Only correct if your supplier hands back something like a `ConcurrentHashMap` and the accumulator is genuinely safe for concurrent mutation. Combined with `UNORDERED` (or an unordered source), this lets a parallel stream skip the merge step entirely.
- `UNORDERED` — the collector doesn't care about encounter order, so the framework is free to reorder work for parallel throughput.
- `IDENTITY_FINISH` — the accumulator type *is* the result type, so the framework can skip calling a finisher and cast directly. Omit this characteristic (and supply a real finisher as the 4th `Collector.of` argument) when your accumulator is an internal working type different from the public result type, e.g. accumulating into a mutable builder and finishing into an immutable value.

## mapMulti vs flatMap

Both turn one input into zero-or-more outputs. `flatMap` requires you to materialize an inner `Stream` per input element even when that stream would hold 0 or 1 items; `mapMulti` (Java 16) instead hands you a `Consumer` to push results into directly, avoiding that intermediate allocation:

```java
// flatMap — allocates a Stream (often Stream.of(...) or Stream.empty()) per input element
stream.flatMap(order -> order.isValid() ? Stream.of(order.total()) : Stream.empty());

// mapMulti — no intermediate Stream allocated; push 0 or 1 (or more) results directly
stream.<Integer>mapMulti((order, consumer) -> {
    if (order.isValid()) consumer.accept(order.total());
});
```

Prefer `flatMap` when the natural output is already a `Stream`/`Collection` (e.g. exploding a `List<Order>` field per customer — `flatMap(Customer::orders)`, no new allocation needed since the stream already exists). Reach for `mapMulti` when you're conditionally emitting a small, bounded number of primitive or simple results per element and would otherwise be wrapping them in throwaway `Stream.of`/`Stream.empty()` calls — it also has primitive-specialized forms (`mapMultiToInt`/`Long`/`Double`) that avoid boxing entirely.
