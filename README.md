# Apache Flink Playground

Hands-on Apache Flink learning environment with 24 progressive exercises — from your first DataStream job to production patterns.

Built with Java 21 and Apache Flink 2.0. Runs locally with Docker or in-process with Flink MiniCluster.

[中文版](README.zh-CN.md)

---

## Quick Start

### Prerequisites

- **JDK 21** — [Download](https://adoptium.net/) (JDK 17 also supported)
- **Docker** — (optional) for the full cluster experience; MiniCluster mode works without it

### Setup

```bash
git clone https://github.com/DanWangDev/flink-playground.git
cd flink-playground

# Set JAVA_HOME (Windows PowerShell)
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.11"

# Verify
./mvnw --version
```

### Run Your First Exercise

**Local mode** (MiniCluster — no Docker, fastest):

```bash
./mvnw compile exec:java -Dexec.mainClass="playground.Main" -Dexec.args="--exercise 01 --local --no-step"
```

**Docker cluster mode** (real Flink jobmanager + taskmanager):

```bash
docker compose up -d
./mvnw package -DskipTests -B
docker compose exec -T jobmanager \
  flink run -c playground.Main \
  /opt/flink/usrlib/flink-playground-1.0.0.jar \
  --exercise 01 --no-step
```

**Interactive step-by-step mode** (pauses between each operation):

```bash
# Add --step to any exercise
./mvnw compile exec:java -Dexec.mainClass="playground.Main" -Dexec.args="--exercise 01 --local --step"
```

### Run All Tests

```bash
./mvnw test
```

---

## Learning Path

24 progressive modules. Each builds on the previous — start at 01 and work forward.

| # | Module | What You'll Learn |
|---|--------|-------------------|
| 01 | **First Job** | StreamExecutionEnvironment, DataStream, map/filter/flatMap, sources, sinks |
| 02 | **Keyed Streams** | keyBy, reduce, aggregate, RichMapFunction, parallel processing |
| 03 | **State Management** | ValueState, ListState, MapState, ReducingState, StateTtlConfig |
| 04 | **Windows** | Tumbling, sliding, session windows; ProcessWindowFunction, triggers |
| 05 | **Event Time & Watermarks** | WatermarkStrategy, BoundedOutOfOrderness, allowed lateness, side outputs |
| 06 | **Checkpointing** | CheckpointConfig, state backends, exactly-once, failure recovery |
| 07 | **Kafka Connector** | KafkaSource, KafkaSink, serialization schemas, consumer groups |
| 08 | **File Connectors** | FileSource, FileSink, CSV/JSON formats, bulk vs streaming reads |
| 09 | **Table API** | TableEnvironment, stream-to-table conversion, changelog streams |
| 10 | **Flink SQL** | DDL, window aggregations, temporal joins, built-in SQL functions |
| 11 | **ProcessFunction** | KeyedProcessFunction, timers, Context API, side outputs |
| 12 | **CEP** | Pattern API, begin/next/followedBy, within(), timeout handling |
| 13 | **Broadcast State** | BroadcastStream, BroadcastProcessFunction, dynamic rule application |
| 14 | **Production Patterns** | Savepoints, rescaling, metrics (Counter/Gauge/Meter), backpressure |
| 15 | **Async I/O** | AsyncDataStream, unorderedWait, CompletableFuture, external service calls |
| 16 | **Stream Joins** | Interval join, window join, multi-stream correlation |
| 17 | **JDBC Connector** | H2 in-memory database, enrichment via JDBC lookups |
| 18 | **Window TVFs** | TUMBLE table function in SQL, window aggregations |
| 19 | **UDF** | User-defined functions with built-in SQL for data transformation |
| 20 | **File Formats** | CSV reading, JSON formatting, FileSink with rolling policies |
| 21 | **Global Windows** | GlobalWindow with CountTrigger and PurgingTrigger |
| 22 | **Restart Strategies** | Stateful processing with configurable restart behavior |
| 23 | **Savepoints** | Running totals across savepoints, state persistence |
| 24 | **Processing Time** | Processing-time vs event-time semantics, timestamp comparison |

Each module includes:
- **`concept.md`** — English concept documentation with diagrams and gotchas
- **`concept_cn.md`** — Chinese (Simplified) translation
- **Exercise runner** — Runnable Java class with structured console output
- **Test** — JUnit 5 test with Flink MiniCluster and AssertJ

---

## Project Structure

```
flink-playground/
├── .github/workflows/ci.yml          # CI: MiniCluster tests + Docker integration
├── docker-compose.yml                # Flink jobmanager + taskmanager + Kafka
├── mvnw / mvnw.cmd                   # Maven wrapper (zero global installs)
├── pom.xml                           # Single-module Maven, Flink 2.0
├── Makefile                          # 15+ convenience targets
├── README.md / README.zh-CN.md
├── .env.example / .gitignore
├── conf/flink-conf.yaml              # Flink configuration overrides
├── data/                             # Shared test data (JSON, CSV)
├── exercises/                        # Documentation per module
│   ├── 01-first-job/
│   │   ├── concept.md
│   │   └── concept_cn.md
│   └── ... (14 modules)
├── src/main/java/playground/
│   ├── Main.java                     # CLI entry point
│   ├── shared/                       # Console, StepPause, CollectingSink, etc.
│   └── exercises/                    # Ex01FirstJob through Ex14ProductionPatterns
└── src/test/java/playground/
    ├── MiniClusterTestBase.java
    ├── helpers/StreamAssertions.java
    └── exercises/                    # One test per exercise
```

---

## Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available targets |
| `make cluster-start` | Start Flink cluster via Docker Compose |
| `make cluster-stop` | Stop Flink cluster |
| `make cluster-status` | Show cluster health (REST API) |
| `make build` | Build fat JAR (`./mvnw package`) |
| `make test` | Run all MiniCluster tests |
| `make test-coverage` | Run tests with JaCoCo coverage |
| `make exercise-01` | Run exercise 01 on Docker cluster |
| `make exercise-local-01` | Run exercise 01 with MiniCluster |

---

## Design Decisions

1. **Java 21 + Flink 2.0** — Java 21 is the latest LTS. Flink 2.0 provides the latest DataStream API with the new unified Sink/SinkWriter interface.
2. **Dual-run mode** — `--local` uses in-process MiniCluster (fast, no Docker); default uses Docker Compose cluster (real Flink).
3. **CollectingSink pattern** — Custom sink collects records into a `List<T>` for deterministic test assertions. No flaky async checks.
4. **Bounded streams** — All exercises use finite data for fast, reproducible results.
5. **Single Maven module** — Each exercise is one Java class extending `ExerciseRunner`. No multi-module complexity.
6. **Interactive step mode** — `--step` flag pauses between operations for learning and inspection.
7. **Bilingual documentation** — Every module has English and Chinese concept docs.
8. **Zero framework dependencies** — Only Flink, JUnit 5, and AssertJ. No Spring, no Lombok.

---

## CI Pipeline

Two-lane strategy on every push and PR:

| Lane | What | Runtime |
|------|------|---------|
| **test** | Compile + MiniCluster tests (JDK 17 & 21) | ~2 min |
| **integration** | Docker Compose cluster + `flink run` all exercises | ~8 min |

Both must be green before merge.

---

## Key Flink Concepts at Play

| Concept | Where |
|---------|-------|
| DataStream API | 01, 02 |
| Keyed state | 02, 03 |
| State backends | 03, 06 |
| Windowing | 04, 18, 21 |
| Event time / watermarks | 05, 24 |
| Checkpointing | 06, 23 |
| Kafka source/sink | 07 |
| File I/O | 08, 20 |
| Table API | 09 |
| Flink SQL | 10, 18 |
| ProcessFunction | 11, 24 |
| CEP patterns | 12 |
| Broadcast state | 13 |
| Savepoints & metrics | 14, 23 |
| Async I/O | 15 |
| Stream joins | 16 |
| JDBC enrichment | 17 |
| SQL window TVFs | 18 |
| Built-in SQL UDFs | 19 |
| JSON/CSV formats | 20 |
| Global windows & triggers | 21 |
| Restart strategies | 22 |
| Processing time semantics | 24 |

---

## License

MIT
