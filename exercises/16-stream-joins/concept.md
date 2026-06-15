# Exercise 16 — Multi-Stream Joins

## What You'll Learn

- Interval join for time-bounded stream correlation
- Keyed stream pairing with keyBy
- Deterministic timestamp assignment for testable joins

---

## Why This Matters

Real-world pipelines process multiple streams (orders + shipments, clicks + impressions). Joining streams is essential for enrichment and correlation.

---

## Core Concepts

Interval join matches events from two streams when the timestamp of the second event falls within a time window relative to the first. Both streams must be keyed on the same key type.

## Gotchas

- Both streams must use the same key type for keyBy
- Interval bounds define which events can match — too wide = large state
- Use deterministic timestamps (not hash-based) for reproducible tests
