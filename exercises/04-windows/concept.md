# Exercise 04 — Windows

## What You'll Learn

- **Tumbling windows** — fixed-size, non-overlapping
- **Sliding windows** — fixed-size, overlapping
- **Session windows** — activity-based, variable-size
- **ProcessWindowFunction** — access to window metadata (start, end time)

---

## Why This Matters

Streams are infinite. Windows slice an infinite stream into finite chunks for aggregation. Without windows, you can't compute a "5-minute average" or "hourly count" — they're the foundation of time-based analytics.

---

## Window Types

| Type | Size | Overlap | Use Case |
|------|------|---------|----------|
| Tumbling | Fixed | None | "every 5 minutes, give me the average" |
| Sliding | Fixed | Yes | "every minute, give me the last 5 minutes" |
| Session | Variable | None | "group events with <2 min gap" |
| Global | Entire stream | N/A | "count all events ever" |

```
Tumbling (size=3):
  [0-3)  [3-6)  [6-9)

Sliding (size=4, slide=2):
  [0-4)   [2-6)   [4-8)

Session (gap=2):
  [1-5]    [8-12]
```

## ProcessWindowFunction

Gives access to window metadata:

```java
.process(new ProcessWindowFunction<IN, OUT, KEY, TimeWindow>() {
    void process(KEY key, Context ctx, Iterable<IN> elements, Collector<OUT> out) {
        long start = ctx.window().getStart();
        long end = ctx.window().getEnd();
        // aggregate elements...
    }
})
```

---

## What You'll Practice

1. Compute average temperature with **tumbling windows** (3s)
2. Compute overlapping averages with **sliding windows** (4s window, 2s slide)
3. Group events by activity with **session windows** (2s gap)

---

## Key Gotchas

1. **Event time vs processing time** — This exercise uses event time. If no watermark is set, windows never close. The DataSources.sensors() helper sets watermarks automatically.

2. **Late events** — Events arriving after the watermark are dropped by default. Use `allowedLateness()` to keep windows open longer.

3. **Window state** — Flink stores all events in window state until the window fires. Large windows = large state.

4. **Duration, not Time** — Use `Duration.ofSeconds(n)` instead of the removed `Time.seconds(n)`.
