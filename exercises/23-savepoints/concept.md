# Exercise 23 — Savepoints & Rescaling

## What You'll Learn

- Running totals that persist across job executions
- State that survives MiniCluster restarts
- Savepoint concepts for production upgrades

---

## Why This Matters

Savepoints are manually-triggered checkpoints that persist after a job stops. They enable planned maintenance: stop job, upgrade Flink, rescale parallelism, restart from savepoint — zero data loss.

---

## Core Concepts

- Savepoints are created manually (`flink savepoint`)
- They persist after job termination
- Jobs can restart from a savepoint with different parallelism
- All keyed state is automatically included

The exercise demonstrates the pattern with a running total that accumulates across execution boundaries.

## Gotchas

- Savepoints are manual, not automatic — requires operational discipline
- In MiniCluster tests, state is lost after `execute()` returns
- For production, always test savepoint compatibility before upgrading Flink versions
