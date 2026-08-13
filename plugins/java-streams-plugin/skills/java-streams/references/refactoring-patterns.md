# Recognizing Stream Refactor Candidates

A pattern-recognition guide for spotting when a nested loop or a chain of `if` statements has the *shape* of a stream pipeline. This is a recognition tool, not an execution mandate — see `SKILL.md`'s Scope section: noticing a candidate is not the same as rewriting it. For existing code, surface what you noticed and propose the change (or ask); only apply this automatically when you're writing new code from scratch.

Table of contents: [Nested loops → flatMap](#nested-loops--flatmap) · [Loop + flag/break → short-circuit terminal ops](#loop--flagbreak--short-circuiting-terminal-ops) · [Loop filling multiple buckets → groupingBy/partitioningBy](#loop-filling-multiple-buckets--groupingbypartitioningby) · [Nested null-check ifs → Optional](#nested-null-check-ifs--optional) · [Chained classification ifs → groupingBy with a classifier](#chained-classification-ifs--groupingby-with-a-classifier) · [When a loop is NOT a good candidate](#when-a-loop-is-not-a-good-candidate) · [Quick signal checklist](#quick-signal-checklist)

## Nested loops → flatMap

The signal: an outer loop over a collection, an inner loop over something reachable from each outer element, both loops feeding into one flat result.

```java
// Before
List<LineItem> allItems = new ArrayList<>();
for (Order order : orders) {
    for (LineItem item : order.getLineItems()) {
        allItems.add(item);
    }
}

// After
List<LineItem> allItems = orders.stream()
    .flatMap(order -> order.getLineItems().stream())
    .toList();
```

The same shape with a condition in the inner loop becomes `flatMap` + `filter`:

```java
// Before
List<LineItem> expensive = new ArrayList<>();
for (Order order : orders) {
    for (LineItem item : order.getLineItems()) {
        if (item.getPrice() > 100) {
            expensive.add(item);
        }
    }
}

// After
List<LineItem> expensive = orders.stream()
    .flatMap(order -> order.getLineItems().stream())
    .filter(item -> item.getPrice() > 100)
    .toList();
```

A **triple**-nested loop building all pairwise combinations of two collections (a cross join) is nested `flatMap`, not a third loop level:

```java
// Before
List<String> pairs = new ArrayList<>();
for (String a : listA) {
    for (String b : listB) {
        pairs.add(a + "-" + b);
    }
}

// After
List<String> pairs = listA.stream()
    .flatMap(a -> listB.stream().map(b -> a + "-" + b))
    .toList();
```

See `advanced-scenarios.md` #5 for the recursive case (arbitrary-depth trees, not just two fixed levels).

## Loop + flag/break → short-circuiting terminal ops

The signal: a `boolean found`/`boolean allMatch`-style variable initialized before the loop, flipped inside it, often paired with a `break`, then checked after the loop ends.

```java
// Before
boolean hasOverdue = false;
for (Invoice invoice : invoices) {
    if (invoice.isOverdue()) {
        hasOverdue = true;
        break;
    }
}

// After
boolean hasOverdue = invoices.stream().anyMatch(Invoice::isOverdue);
```

The three variants map onto the three matching terminal ops directly — recognize which one by what the loop is actually testing:
- "does at least one element satisfy X, stop as soon as found" → `anyMatch`
- "do all elements satisfy X, stop as soon as one fails" → `allMatch`
- "confirm no element satisfies X" → `noneMatch`

A loop that returns the **first matching element itself**, not just a boolean, is `filter(...).findFirst()`:

```java
// Before
Customer target = null;
for (Customer c : customers) {
    if (c.getId().equals(targetId)) {
        target = c;
        break;
    }
}

// After
Optional<Customer> target = customers.stream()
    .filter(c -> c.getId().equals(targetId))
    .findFirst();
```

Note the return type changes from a nullable `Customer` to `Optional<Customer>` — that's usually a genuine improvement (the "might not be found" case becomes visible in the type), but it does mean callers need to handle `Optional` rather than checking for `null`, so this is a case worth calling out explicitly if converting existing code, not just swapping the body silently.

## Loop filling multiple buckets → groupingBy/partitioningBy

The signal: one loop with an `if`/`else` inside that appends the same element to different lists depending on a condition.

```java
// Before — binary split
List<Order> paid = new ArrayList<>();
List<Order> unpaid = new ArrayList<>();
for (Order order : orders) {
    if (order.isPaid()) {
        paid.add(order);
    } else {
        unpaid.add(order);
    }
}

// After
Map<Boolean, List<Order>> byPaid = orders.stream()
    .collect(Collectors.partitioningBy(Order::isPaid));
List<Order> paid = byPaid.get(true);
List<Order> unpaid = byPaid.get(false);
```

More than two buckets (an `if`/`else if`/`else if` chain, or a `switch`, choosing which list to append to) is `groupingBy` with that same classification logic as the classifier function — see the next section and `collectors.md`'s `groupingBy` coverage for downstream composition (counting per bucket, summing per bucket, etc., instead of just collecting each bucket into a list).

## Nested null-check ifs → Optional

The signal: an `if (x != null) { ... if (y != null) { ... } }` pyramid navigating from one object to a nested field.

```java
// Before
String city = "Unknown";
if (order != null) {
    Customer customer = order.getCustomer();
    if (customer != null) {
        Address address = customer.getAddress();
        if (address != null && address.getCity() != null) {
            city = address.getCity();
        }
    }
}

// After
String city = Optional.ofNullable(order)
    .map(Order::getCustomer)
    .map(Customer::getAddress)
    .map(Address::getCity)
    .orElse("Unknown");
```

This is `java.util.Optional`, not `java.util.stream`, but the two are designed to compose — when the null-safe navigation needs to run once per element of a *collection* (filtering out any element that fails the chain, rather than falling back to a default for one object), it becomes a stream with `flatMap(x -> Optional.ofNullable(...).stream())` per stage. See the worked example in the "single object vs. a collection of objects" distinction — this is a common enough question that it's worth getting right which of the two applies before writing anything.

## Chained classification ifs → groupingBy with a classifier

The signal: the same `if`/`else if` ladder repeated inside a loop to bucket each element by some derived category.

```java
// Before
Map<String, List<Customer>> tierMap = new HashMap<>();
for (Customer c : customers) {
    String tier;
    if (c.getTotalSpend() > 10000) {
        tier = "gold";
    } else if (c.getTotalSpend() > 1000) {
        tier = "silver";
    } else {
        tier = "bronze";
    }
    tierMap.computeIfAbsent(tier, k -> new ArrayList<>()).add(c);
}

// After
Map<String, List<Customer>> tierMap = customers.stream()
    .collect(Collectors.groupingBy(c -> {
        if (c.getTotalSpend() > 10000) return "gold";
        if (c.getTotalSpend() > 1000) return "silver";
        return "bronze";
    }));
```

The important thing to notice: the *classification* `if`/`else if` chain doesn't go away — it's still an `if`/`else if` chain (or could become a `switch` expression), just moved inside a small classifier lambda/method reference. What streams remove here is the *looping and bucket-management boilerplate* (`computeIfAbsent`, manually creating each `ArrayList`), not the branching logic itself. Don't force the classification logic itself into a stream construct just because it's now inside a lambda — a multi-line `if`/`else if` or `switch` body inside `groupingBy(...)` is normal and fine; extract it to a named method if it gets long enough to hurt readability inline.

## When a loop is NOT a good candidate

Being honest about the limits keeps this a useful recognition tool instead of a hammer looking for nails. Loops that are usually *better left as loops*:

- **Needs adjacent-element access** (comparing `list.get(i)` to `list.get(i+1)`, computing a running diff/delta). Streams have no built-in "look at the previous element" operation — forcing this into a stream usually means smuggling an index or mutable state back in through `IntStream.range` or an `AtomicReference`, which is often *less* readable than the original loop, not more. If this shows up often enough to matter, a small dedicated helper method is usually clearer than contorting a stream to do it.
- **Multiple, semantically different early exits.** A loop with several `return`/`break` points that each mean something different (not just "found it, stop") rarely collapses cleanly into one stream pipeline — you'd be reaching for exceptions-as-control-flow or multiple partial pipelines, both worse than the loop.
- **Heavy, order-dependent mutation of external state across iterations** (a state machine advancing step by step, an accumulator whose next value depends on complex conditional logic involving several prior values, not just a simple running total). This is `reduce`/`collect` territory in principle, but if the accumulation logic is genuinely complex, a well-named loop with a clearly-typed accumulator variable can be more honest about what's happening than a dense reduce lambda — see `advanced-scenarios.md` #13 for where forcing complex mutable accumulation into `reduce` actively goes wrong under parallelism, which is a sharper version of the same "don't force it" lesson.
- **`continue` used to skip to the next outer-loop iteration from deep inside a nested loop.** This is really just `filter`/`anyMatch` in disguise most of the time, but double-check what the `continue` is actually testing before assuming it maps cleanly — if it's skipping based on state accumulated so far in the loop (not a pure per-element predicate), it may not translate.

When in doubt, the question to ask is the same one `SKILL.md`'s intro poses about the whole API: does the stream version make the *intent* clearer, or does it just make the *syntax* different? If it's the latter, the loop was probably fine.

## Quick signal checklist

A fast pattern-match to run when reading unfamiliar loop-heavy code, roughly in order of how common each is:

- `for` inside a `for`, both building one flat collection → **`flatMap`**
- `boolean` flag set inside a loop, tested after, possibly with a `break` → **`anyMatch`/`allMatch`/`noneMatch`**
- A variable holding "the first match," set once then `break` → **`filter(...).findFirst()`**
- Two or more `ArrayList`s filled conditionally from the same loop → **`partitioningBy`** (2 buckets) or **`groupingBy`** (N buckets)
- `if (x != null) { if (y != null) { ... } }` pyramid down to one value → **`Optional.ofNullable(...).map(...).map(...)`**
- Same pyramid, but running once per element of a collection, filtering out failures → **`flatMap(x -> Optional.ofNullable(...).stream())`**
- A running numeric total, count, min, or max accumulated across the loop → **`Collectors.summingX`/`countingX`/`summaryStatistics()`** (see `collectors.md`)
- `list.get(i)` compared against `list.get(i-1)` or `i+1`, or a loop that otherwise depends on position — **usually not a good candidate**, see above
