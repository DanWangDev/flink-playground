# Exercise 24 — Processing Time Semantics

## What You'll Learn

- Processing time vs event time comparison
- Timestamp and watermark access via ProcessFunction
- Deterministic vs non-deterministic time semantics

---

## Why This Matters

Flink supports three time semantics: event time (when it happened), processing time (when processed), and ingestion time. Understanding the trade-offs is essential for correct stream processing.

---

## Core Concepts

```java
stream.process(new ProcessFunction<Event, String>() {
    void processElement(Event e, Context ctx, Collector<String> out) {
        long procTime = ctx.timerService().currentProcessingTime();
        long wm = ctx.timerService().currentWatermark();
        out.collect(String.format("proc=%d wm=%d", procTime, wm));
    }
});
```

- Processing time: wall-clock time, non-deterministic, always available
- Event time: embedded in the data, deterministic, requires watermarks
- Watermark: tracks event-time progress, null if no watermark strategy is set

## Gotchas

- Processing time results vary between runs — not suitable for testing
- Event time with fixed timestamps gives reproducible results
- For deterministic tests, use event time with `WatermarkStrategy.forBoundedOutOfOrderness`
