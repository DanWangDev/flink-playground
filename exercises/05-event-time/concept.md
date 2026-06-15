# Exercise 05 — Event Time & Watermarks

## What You'll Learn

- **Event time** vs processing time — why timestamps matter
- **Watermarks** — how Flink tracks progress in event time
- **Side outputs** — handling late data gracefully

---

## Why This Matters

Real-world data is rarely perfectly ordered. Events arrive late due to network delays, mobile offline buffering, or upstream backpressure. Event time processing ensures correct results regardless of arrival order — critical for financial calculations, analytics, and any use case where "when it happened" matters more than "when we received it."

---

## Core Concepts

### Event Time vs Processing Time

| Time | Definition | Use Case |
|------|-----------|----------|
| Event time | Timestamp embedded in the event | Accurate analytics, replay |
| Processing time | Wall-clock time when event is processed | Latency-sensitive, approximate |
| Ingestion time | Time when event enters Flink | Compromise between the two |

### Watermarks

A watermark is a threshold that declares: "No event older than T will arrive."

```
Events:         E1(1000)  E2(2000)  E4(4000)  E3(1500)  E5(5000)
Watermarks:        W(1999)      W(3999)                       W(4999)
Windows fire:                     [0-3000)     [3000-6000)
```

E3 arrives late (out of order) but within the 2s bounded-out-of-order tolerance, so it still lands in the correct window.

### Side Outputs

Late events (arriving after the watermark + allowed lateness) can be redirected to a side output instead of being silently dropped:

```java
OutputTag<Event> lateTag = new OutputTag<Event>("late") {};
stream.keyBy(...)
      .window(...)
      .sideOutputLateData(lateTag)
      .process(...);

DataStream<Event> lateStream = result.getSideOutput(lateTag);
```

---

## What You'll Practice

1. Process sensor readings using event time with watermarks
2. Configure bounded-out-of-order tolerance
3. Redirect late data to a side output

---

## Key Gotchas

1. **No watermark = windows never close** — Without a watermark strategy, event-time windows never fire. Always assign timestamps and watermarks.

2. **Watermark is global** — In multi-source jobs, the watermark is the minimum across all partitions. One slow partition delays the entire job.

3. **`BoundedOutOfOrderness` is per-partition** — If partition A's data is always 10s behind partition B's, partition A's watermark advances slower, causing overall delay.

4. **Testing event time** — Use fixed timestamps and bounded data for deterministic tests. Avoid `System.currentTimeMillis()`.

5. **Watermark delay** — Use `Duration.ofSeconds()` for watermark delay, not `Time.seconds()`.
