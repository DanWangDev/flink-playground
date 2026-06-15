# Exercise 14 — Production Patterns

## What You'll Learn
- Counter for event counting, Gauge for running totals
- Stateful processing patterns that survive failures
- Operational visibility through metrics

## Core Concepts
```java
Counter c = getRuntimeContext().getMetricGroup().counter("events");
c.inc();  // increment on each event

getRuntimeContext().getMetricGroup().gauge("runningTotal",
    (Gauge<Long>) () -> lastValue);
```

## Gotchas
- Metrics are per-subtask — aggregate in your monitoring system
- Gauges must be thread-safe — they're read from a different thread
- State grows unbounded without TTL — configure StateTtlConfig for production
