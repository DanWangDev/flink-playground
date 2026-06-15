# Exercise 21 — Global Windows & Custom Triggers

## What You'll Learn

- GlobalWindow — a window that never ends by default
- CountTrigger — fire after N elements accumulate
- PurgingTrigger — clear window state after each firing
- Combining triggers for custom window behavior

---

## Why This Matters

Not all windows are time-based. Global windows with custom triggers enable count-based batching, session-like grouping, or any custom firing logic you need.

---

## Core Concepts

```java
stream.keyBy(...)
    .window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(CountTrigger.of(2)))
    .process(...);
```

- GlobalWindow creates a single window per key
- CountTrigger fires every N elements
- PurgingTrigger clears all elements after firing (stateless window)
- Without purge, elements accumulate indefinitely

## Gotchas

- GlobalWindow without a trigger never fires — always set a trigger
- Without PurgingTrigger, state grows unboundedly (memory leak)
- Combine triggers for complex policies: time OR count, count AND size, etc.
