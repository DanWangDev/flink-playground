# 练习 01 — 你的第一个 Flink 作业

## 你将学到

- 什么是 **StreamExecutionEnvironment** 以及为什么它是每个 Flink 作业的入口
- 如何从内存数据创建 **DataStream**
- 如何应用**无状态转换**：`map`、`filter`、`flatMap`
- 如何使用自定义 sink **收集结果**进行测试

---

## 为什么这很重要

每个 Flink 作业都以相同的方式开始：创建环境、定义转换流水线、然后执行。理解这个基础会使后续的每个概念（状态、窗口、连接器）更容易学习，因为它们都插入到相同的流水线模型中。

---

## 核心概念

### StreamExecutionEnvironment

`StreamExecutionEnvironment` 是所有 Flink 流处理作业的**入口点**。它管理：

- **并行度** — 多少并行子任务处理你的流
- **检查点配置** — Flink 快照状态的频率
- **作业执行** — 调用 `env.execute()` 将流水线提交到 Flink 运行时

```java
// 本地执行（MiniCluster — 无需 Docker，进程内运行）
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

// 远程执行（提交到 Flink 集群）
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

### DataStream<T>

`DataStream<T>` 表示一个**类型为 T 的元素序列**。它可以是：

- **有界的** — 有限数据集（文件、内存集合）
- **无界的** — 无限流（Kafka 主题、套接字）

| 数据源 | 描述 |
|--------|------|
| `env.fromData(...)` | 从内存元素创建 |
| `env.readTextFile("path")` | 从文件读取 |
| `env.addSource(new KafkaSource<>(...))` | 从 Kafka 读取 |
| `env.socketTextStream("host", port)` | 从套接字读取 |

### Map、Filter、FlatMap

这些是**无状态转换** — 每个元素独立处理：

| 操作符 | 输入 → 输出 | 用例 |
|--------|------------|------|
| `map` | 1 → 1 | 转换每个元素（如大写、解析 JSON） |
| `filter` | 1 → 0 或 1 | 保留满足条件的元素 |
| `flatMap` | 1 → 0..N | 拆分、展开或扁平化元素 |

```
┌─────────┐    map(x → f(x))    ┌─────────┐
│ "hello" │ ──────────────────→ │ "HELLO" │
└─────────┘                     └─────────┘

┌─────────┐  filter(x → x > 5)  ┌─────────┐
│    3    │ ──────────────────→ │  (跳过)  │
│    7    │ ──────────────────→ │    7    │
└─────────┘                     └─────────┘

┌───────────────┐ flatMap(split) ┌──────┐
│ "hello world" │ ─────────────→ │ hello│
└───────────────┘                │ world│
                                 └──────┘
```

### CollectingSink

对于练习和测试，我们使用 `CollectingSink<T>` 将流输出捕获到内存中的 `List<T>`：

```java
CollectingSink<String> sink = new CollectingSink<>();
stream.addSink(sink);
env.execute("my-job");
List<String> results = sink.getValues();  // 所有收集的记录
```

在生产环境中，你会将其替换为真正的 sink（Kafka、数据库、文件）。

---

## 你将练习

1. 创建 `StreamExecutionEnvironment`
2. 从内存中的句子构建 `DataStream<String>`
3. 使用 `flatMap` 将句子拆分为单词
4. 使用 `filter` 移除短单词
5. 使用 `map` 将单词首字母大写
6. 使用 `CollectingSink` 收集结果
7. 使用 `env.execute()` 执行作业

---

## 常见陷阱

1. **始终调用 `env.execute()`** — Flink 作业是延迟求值的。只有在调用 `execute()` 时才构建流水线。忘记调用意味着什么都不会发生。

2. **返回类型擦除** — 使用 lambda 时，Java 的类型擦除可能导致问题。使用 `.returns(String.class)` 显式声明输出类型：
   ```java
   stream.flatMap((String s, Collector<String> out) -> { ... })
         .returns(String.class);  // lambda 必需
   ```

3. **ExecutionEnvironment 是一次性的** — `execute()` 返回后，环境不能重复使用。每个作业需要创建一个新的。

4. **为操作符命名** — 在每个转换上使用 `.name("描述性名称")`。名称会出现在 Flink Web UI 中，使调试更加容易。

5. **测试中使用 MiniCluster** — 使用 `createLocalEnvironment(1)` 进行测试。它在进程内运行，快速且确定性。无需 Docker。
