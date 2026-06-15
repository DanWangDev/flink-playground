# Exercise 01 — Your First Flink Job

## What You'll Learn

- What a **StreamExecutionEnvironment** is and why it's the entry point for every Flink job
- How to create a **DataStream** from in-memory data
- How to apply **stateless transformations**: `map`, `filter`, `flatMap`
- How to **collect results** for testing with a custom sink

---

## Why This Matters

Every Flink job starts the same way: create an environment, define a pipeline of transformations, and execute. Understanding this foundation makes every subsequent concept (state, windows, connectors) easier to learn because they all plug into the same pipeline model.

---

## Core Concepts

### StreamExecutionEnvironment

The `StreamExecutionEnvironment` is the **entry point** for all Flink streaming jobs. It manages:

- **Parallelism** — how many parallel subtasks process your streams
- **Checkpointing configuration** — how often Flink snapshots state
- **Job execution** — calling `env.execute()` submits the pipeline to the Flink runtime

```java
// Local execution (MiniCluster — no Docker, in-process)
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

// Remote execution (submits to a Flink cluster)
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

### DataStream<T>

A `DataStream<T>` represents a **sequence of elements of type T**. It can be:

- **Bounded** — a finite dataset (file, in-memory collection)
- **Unbounded** — an infinite stream (Kafka topic, socket)

| Source | Description |
|--------|-------------|
| `env.fromData(...)` | Create from in-memory elements |
| `env.readTextFile("path")` | Read from a file |
| `env.addSource(new KafkaSource<>(...))` | Read from Kafka |
| `env.socketTextStream("host", port)` | Read from a socket |

### Map, Filter, FlatMap

These are **stateless transformations** — each element is processed independently:

| Operator | Input → Output | Use Case |
|----------|---------------|----------|
| `map` | 1 → 1 | Transform each element (e.g., uppercase, parse JSON) |
| `filter` | 1 → 0 or 1 | Keep elements matching a condition |
| `flatMap` | 1 → 0..N | Split, expand, or flatten elements |

```
┌─────────┐    map(x → f(x))    ┌─────────┐
│ "hello" │ ──────────────────→ │ "HELLO" │
└─────────┘                     └─────────┘

┌─────────┐  filter(x → x > 5)  ┌─────────┐
│    3    │ ──────────────────→ │  (skip) │
│    7    │ ──────────────────→ │    7    │
└─────────┘                     └─────────┘

┌───────────────┐ flatMap(split) ┌──────┐
│ "hello world" │ ─────────────→ │ hello│
└───────────────┘                │ world│
                                 └──────┘
```

### CollectingSink

For exercises and tests, we use a `CollectingSink<T>` that captures stream output into an in-memory `List<T>`:

```java
CollectingSink<String> sink = new CollectingSink<>();
stream.addSink(sink);
env.execute("my-job");
List<String> results = sink.getValues();  // all collected records
```

In production, you'd replace this with a real sink (Kafka, database, file).

---

## What You'll Practice

1. Create a `StreamExecutionEnvironment`
2. Build a `DataStream<String>` from in-memory sentences
3. Use `flatMap` to split sentences into words
4. Use `filter` to remove short words
5. Use `map` to capitalize words
6. Collect results with `CollectingSink`
7. Execute the job with `env.execute()`

---

## Key Gotchas

1. **Always call `env.execute()`** — Flink jobs are lazily evaluated. The pipeline is only built when `execute()` is called. Forgetting it means nothing happens.

2. **Return type erasure** — When using lambdas, Java's type erasure can cause issues. Use `.returns(String.class)` to explicitly declare the output type:
   ```java
   stream.flatMap((String s, Collector<String> out) -> { ... })
         .returns(String.class);  // required for lambdas
   ```

3. **ExecutionEnvironment is one-shot** — After `execute()` returns, the environment cannot be reused. Create a new one for each job.

4. **Name your operators** — Use `.name("descriptive-name")` on each transformation. Names appear in the Flink Web UI, making debugging much easier.

5. **MiniCluster for tests** — Use `createLocalEnvironment(1)` for tests. It runs in-process, is fast, and deterministic. No Docker needed.
