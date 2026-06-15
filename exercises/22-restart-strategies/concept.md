# Exercise 22 — Restart Strategies

## What You'll Learn

- Stateful processing that survives task failures
- Counters tracked across restarts in MiniCluster
- Configuring restart behavior at the cluster level

---

## Why This Matters

Production jobs run for weeks. When a task fails, Flink restarts it automatically. The restart strategy determines how many retries and how long to wait. Combined with checkpointing, this provides resilience without data loss.

---

## Core Concepts

Restart strategies are configured at the cluster level (flink-conf.yaml) or via CLI:

- **fixed-delay**: retry N times with M seconds between
- **failure-rate**: retry as long as failure rate stays below threshold
- **exponential-delay**: increasing delays between retries

In code, the key to fault tolerance is checkpointing + state backend. State declared via ValueState, ListState, or MapState is automatically checkpointed and restored on restart.

## Gotchas

- Without checkpointing, state is lost on restart
- Restart strategies only apply to task failures, not job submission errors
- In MiniCluster tests, restart happens in-process — real behavior requires a cluster
