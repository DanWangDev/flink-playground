# 练习 06 — 检查点与容错

## 你将学到

- 如何在 Flink 中**启用检查点**
- 如何**自动对状态进行检查点**（无需更改代码）
- **精确一次**和**至少一次**之间的区别
- Flink 如何从故障中**恢复**

---

## 为什么这很重要

流处理作业运行数天、数周或永远。硬件故障、网络分区、进程崩溃。检查点是安全网 — 它定期对所有状态进行快照，以便在故障后作业可以从最后一个快照恢复，零数据丢失。

---

## 核心概念

### 检查点配置

```java
env.enableCheckpointing(5000);  // 每 5 秒检查点
env.getCheckpointConfig().setCheckpointingMode(EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
env.getCheckpointConfig().setCheckpointTimeout(60000);
```

### 精确一次 vs 至少一次

| 模式 | 保证 | 性能 |
|------|------|------|
| EXACTLY_ONCE | 无重复，无数据丢失 | 稍慢（屏障对齐） |
| AT_LEAST_ONCE | 无数据丢失，可能重复 | 更快 |

### 状态后端

| 后端 | 存储 | 用例 |
|------|------|------|
| HashMapStateBackend | Java 堆 | 测试，小状态 |
| EmbeddedRocksDBStateBackend | 本地磁盘 | 大状态（TB+） |

通过 `ValueState`、`ListState` 或 `MapState` 声明的状态会**自动**进行检查点。无需额外代码。

---

## 你将练习

1. 启用 EXACTLY_ONCE 模式的检查点
2. 运行在故障中幸存的有状态处理
3. 观察状态在多次执行中被保留

---

## 常见陷阱

1. **检查点间隔很重要** — 太频繁：开销大。太不频繁：故障时需重放更多数据。1-10 秒是典型的。

2. **所有数据源必须支持所选模式** — Kafka 数据源支持 EXACTLY_ONCE；某些自定义数据源仅支持 AT_LEAST_ONCE。

3. **状态是键控的** — 检查点只捕获键控状态。操作符状态需要实现 `CheckpointedFunction`。

4. **MiniCluster 会清理** — 在测试模式下，`execute()` 返回后检查点将被丢弃。没有持久文件。
