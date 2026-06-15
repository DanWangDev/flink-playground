# Exercise 02 — Keyed Streams

## What You'll Learn

- How `keyBy` partitions a stream for parallel processing
- How to use `reduce` for running aggregation per key
- How to use `RichMapFunction` for per-key state
- How to use Flink Tuples for structured intermediate results

---

## Why This Matters

`keyBy` is the gateway to stateful stream processing. Without it, every operator sees every record independently. With it, all records sharing a key land on the same parallel subtask, enabling per-key aggregation, counting, and eventually windowing and state management.

---

## Core Concepts

### keyBy

```
Before keyBy:
  Subtask 0: [cust-a, cust-b, cust-c, cust-a, cust-b, ...]
  Subtask 1: [cust-c, cust-a, cust-b, cust-c, cust-a, ...]

After keyBy(customerId):
  Subtask 0: [cust-a, cust-a, cust-a, cust-a]   ← all cust-a records
  Subtask 1: [cust-b, cust-b, cust-b, cust-c, cust-c, cust-c]  ← all cust-b + cust-c
```

Key selectors:
```java
// Lambda key selector (most common)
stream.keyBy(order -> order.customerId())

// Field-based (for Tuple types)
stream.keyBy(value -> value.f0)
```

### reduce

`reduce` combines the current element with the previous reduced value:

```java
stream.keyBy(o -> o.customerId())
      .reduce((acc, next) -> new Order(acc.id(), acc.customer(), acc.amount() + next.amount()));
```

State: Flink maintains the latest reduced value per key. On restore, only the final value is recovered.

### RichMapFunction

`RichMapFunction` gives access to `RuntimeContext` (subtask index, parallelism, metrics). Useful for per-instance counters and initialization:

```java
stream.keyBy(o -> o.customerId())
      .map(new RichMapFunction<Order, String>() {
          private int count;

          @Override
          public void open(Configuration config) {
              count = 0;  // initialize per subtask
          }

          @Override
          public String map(Order order) {
              count++;
              return String.format("%s: order #%d", order.customerId(), count);
          }
      });
```

### Flink Tuples

Convenient for intermediate results without creating custom POJOs:

```java
Tuple2<String, Double> pair = Tuple2.of("cust-a", 100.0);
String id = pair.f0;   // = "cust-a"
Double amt = pair.f1;  // = 100.0
```

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| keyBy | Logical partitioning by key selector |
| reduce | Running fold: (acc, next) → acc |
| RichMapFunction | Map with lifecycle (open, close) and RuntimeContext |
| Tuple2 | Lightweight 2-field container |
| Parallelism | Number of parallel subtasks processing the stream |

---

## What You'll Practice

1. Partition order events by `customerId` with `keyBy`
2. Sum order amounts per customer with `reduce`
3. Count orders per customer with `RichMapFunction`
4. Aggregate structured data with Tuple2 + keyBy + reduce

---

## Key Gotchas

1. **keyBy doesn't load balance** — If one key has 90% of data, that subtask becomes a bottleneck ("hot key" problem).

2. **Lambda type erasure** — When using lambdas with keyBy or reduce, Flink may need explicit type hints via `.returns()`.

3. **reduce is stateful** — The accumulated value per key is stored in Flink's state backend. No extra state declaration needed for simple reduce.

4. **RichFunction.open() vs constructor** — Initialize per-subtask resources in `open()`, not the constructor. `open()` runs on each parallel instance.

5. **RichMapFunction state is not fault-tolerant** — Unlike `ValueState`, plain instance fields (`private int count`) are NOT checkpointed. They reset on failure.
