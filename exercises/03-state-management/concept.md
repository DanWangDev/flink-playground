# Exercise 03 — State Management

## What You'll Learn

- **ValueState** — single value per key (counters, last-seen)
- **ListState** — accumulating list per key (session events, history)
- **MapState** — key-value pairs per key (counts per category)
- How Flink manages state behind the scenes

---

## Why This Matters

State transforms a stateless stream processor into a stateful one. Without state, every event is processed in isolation. With state, you can count, accumulate, join, and remember — the foundation for real-time analytics, fraud detection, and session analysis.

---

## State Types

| Type | Structure | Use Case |
|------|-----------|----------|
| `ValueState<V>` | Single value per key | Counter, last event, flag |
| `ListState<E>` | Ordered list per key | Session events, history |
| `MapState<K,V>` | HashMap per key | Category counts, cache |
| `ReducingState<V>` | Single value with reduce | Running aggregates |

All are obtained from `RuntimeContext` inside a `RichFunction` or `ProcessFunction`.

---

## How It Works

```
keyBy(userId) → ValueState<Integer>
  user-1: [value=3]
  user-2: [value=1]
  user-3: [value=2]

keyBy(userId) → ListState<String>
  user-1: ["page-a", "page-c", "page-b"]
  user-2: ["page-b", "page-a", "page-c"]

keyBy(userId) → MapState<String,Integer>
  user-1: {"page-a": 1, "page-b": 2, "page-c": 1}
```

Flink stores state in the configured state backend:
- **HashMapStateBackend** — In-memory, fast, for testing/small state
- **EmbeddedRocksDBStateBackend** — On-disk, for large state (TB+)

State is checkpointed for fault tolerance — on recovery, Flink restores the exact state from the last checkpoint.

---

## What You'll Practice

1. Count clicks per user with `ValueState`
2. Accumulate visited pages with `ListState`
3. Count per-page per-user with `MapState`

---

## Key Gotchas

1. **State is keyed** — state is only accessible inside a `keyBy` + `KeyedProcessFunction`. You cannot access state without keying first.

2. **State descriptors** — Always declare state in `open()`, not inline. The descriptor tells Flink the name and type.

3. **Null handling** — `ValueState.value()` returns `null` if never set. Always check for null before using.

4. **State TTL** — Configure `StateTtlConfig` to auto-cleanup stale state. Without TTL, state grows unboundedly.

5. **RocksDB serialization** — With RocksDB, every state access reads/writes to disk. Batch reads for better performance.
