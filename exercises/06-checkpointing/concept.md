# Exercise 06 — Checkpointing & Fault Tolerance

## What You'll Learn

- How to **enable checkpointing** in Flink
- How **state is automatically checkpointed** (no code changes needed)
- The difference between **exactly-once** and **at-least-once**
- How Flink **recovers** from failures

---

## Why This Matters

Stream processing jobs run for days, weeks, or forever. Hardware fails, networks partition, processes crash. Checkpointing is the safety net — it takes periodic snapshots of all state so that after a failure, the job can resume from the last snapshot with zero data loss.

---

## Core Concepts

### Checkpoint Configuration

```java
env.enableCheckpointing(5000);  // checkpoint every 5 seconds
env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
env.getCheckpointConfig().setCheckpointTimeout(60000);
```

### Exactly-Once vs At-Least-Once

| Mode | Guarantee | Performance |
|------|-----------|-------------|
| EXACTLY_ONCE | No duplicates, no data loss | Slightly slower (barrier alignment) |
| AT_LEAST_ONCE | No data loss, possible duplicates | Faster |

### State Backends

| Backend | Storage | Use Case |
|---------|---------|----------|
| HashMapStateBackend | Java heap | Testing, small state |
| EmbeddedRocksDBStateBackend | Local disk | Large state (TB+) |

State declared via `ValueState`, `ListState`, or `MapState` is **automatically** checkpointed. No extra code needed.

---

## What You'll Practice

1. Enable checkpointing with EXACTLY_ONCE mode
2. Run stateful processing that survives failure
3. Observe that state is preserved across executions

---

## Key Gotchas

1. **Checkpoint interval matters** — Too frequent: overhead. Too infrequent: more data to replay on failure. 1-10 seconds is typical.

2. **All sources must support the chosen mode** — Kafka sources support EXACTLY_ONCE; some custom sources only support AT_LEAST_ONCE.

3. **State is keyed** — Checkpointing only captures keyed state. Operator state needs `CheckpointedFunction` implementation.

4. **MiniCluster cleans up** — In test mode, checkpoints are discarded after `execute()` returns. No persistent files.
