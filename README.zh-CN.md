# Apache Flink 练习场

Apache Flink 动手学习环境，包含 24 个渐进式练习 — 从你的第一个 DataStream 作业到生产模式。

基于 Java 21 和 Apache Flink 2.0 构建。可通过 Docker 本地运行，或使用 Flink MiniCluster 进程内运行。

[English](README.md)

---

## 快速开始

### 前置条件

- **JDK 21** — [下载](https://adoptium.net/)（也支持 JDK 17）
- **Docker** — （可选）用于完整的集群体验；MiniCluster 模式无需 Docker

### 环境搭建

```bash
git clone https://github.com/DanWangDev/flink-playground.git
cd flink-playground

# 设置 JAVA_HOME（Windows PowerShell）
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21.0.11"

# 验证
./mvnw --version
```

### 运行第一个练习

**本地模式**（MiniCluster — 无需 Docker，最快）：

```bash
./mvnw compile exec:java -Dexec.mainClass="playground.Main" -Dexec.args="--exercise 01 --local --no-step"
```

**Docker 集群模式**（真实的 Flink jobmanager + taskmanager）：

```bash
docker compose up -d
./mvnw package -DskipTests -B
docker compose exec -T jobmanager \
  flink run -c playground.Main \
  /opt/flink/usrlib/flink-playground-1.0.0.jar \
  --exercise 01 --no-step
```

**交互式逐步模式**（每个操作之间暂停）：

```bash
# 在任何练习中添加 --step
./mvnw compile exec:java -Dexec.mainClass="playground.Main" -Dexec.args="--exercise 01 --local --step"
```

### 运行所有测试

```bash
./mvnw test
```

---

## 学习路径

24 个渐进式模块。每个模块都建立在之前的基础上 — 从 01 开始，逐步推进。

| # | 模块 | 你将学到 |
|---|------|----------|
| 01 | **第一个作业** | StreamExecutionEnvironment、DataStream、map/filter/flatMap、数据源、数据汇 |
| 02 | **键控流** | keyBy、reduce、aggregate、RichMapFunction、并行处理 |
| 03 | **状态管理** | ValueState、ListState、MapState、ReducingState、StateTtlConfig |
| 04 | **窗口** | 滚动窗口、滑动窗口、会话窗口；ProcessWindowFunction、触发器 |
| 05 | **事件时间与水位线** | WatermarkStrategy、BoundedOutOfOrderness、允许延迟、侧输出 |
| 06 | **检查点** | CheckpointConfig、状态后端、精确一次、故障恢复 |
| 07 | **Kafka 连接器** | KafkaSource、KafkaSink、序列化模式、消费者组 |
| 08 | **文件连接器** | FileSource、FileSink、CSV/JSON 格式、批量 vs 流式读取 |
| 09 | **Table API** | TableEnvironment、流表转换、变更日志流 |
| 10 | **Flink SQL** | DDL、窗口聚合、时间连接、内置 SQL 函数 |
| 11 | **ProcessFunction** | KeyedProcessFunction、定时器、Context API、侧输出 |
| 12 | **CEP** | Pattern API、begin/next/followedBy、within()、超时处理 |
| 13 | **广播状态** | BroadcastStream、BroadcastProcessFunction、动态规则应用 |
| 14 | **生产模式** | Savepoints、扩缩容、指标（Counter/Gauge/Meter）、背压 |
| 15 | **异步 I/O** | AsyncDataStream、unorderedWait、CompletableFuture、外部服务调用 |
| 16 | **流连接** | 区间连接、窗口连接、多流关联 |
| 17 | **JDBC 连接器** | H2 内存数据库、通过 JDBC 查询进行数据丰富 |
| 18 | **窗口 TVF** | SQL 中的 TUMBLE 表函数、窗口聚合 |
| 19 | **UDF** | 使用内置 SQL 函数进行数据转换 |
| 20 | **文件格式** | CSV 读取、JSON 格式化、带滚动策略的 FileSink |
| 21 | **全局窗口** | GlobalWindow 配合 CountTrigger 和 PurgingTrigger |
| 22 | **重启策略** | 可配置重启行为的有状态处理 |
| 23 | **保存点** | 跨保存点的运行总计、状态持久化 |
| 24 | **处理时间** | 处理时间与事件时间语义对比、时间戳比较 |

每个模块包含：
- **`concept.md`** — 英文概念文档，包含图表和常见陷阱
- **`concept_cn.md`** — 中文概念文档
- **练习运行器** — 可运行的 Java 类，带有结构化控制台输出
- **测试** — 使用 Flink MiniCluster 和 AssertJ 的 JUnit 5 测试

---

## 项目结构

```
flink-playground/
├── .github/workflows/ci.yml          # CI：MiniCluster 测试 + Docker 集成
├── docker-compose.yml                # Flink jobmanager + taskmanager + Kafka
├── mvnw / mvnw.cmd                   # Maven wrapper（无需全局安装）
├── pom.xml                           # 单模块 Maven，Flink 2.0
├── Makefile                          # 15+ 便捷命令
├── README.md / README.zh-CN.md
├── .env.example / .gitignore
├── conf/flink-conf.yaml              # Flink 配置覆盖
├── data/                             # 共享测试数据（JSON、CSV）
├── exercises/                        # 每个模块的文档
│   ├── 01-first-job/
│   │   ├── concept.md
│   │   └── concept_cn.md
│   └── ...（共 14 个模块）
├── src/main/java/playground/
│   ├── Main.java                     # CLI 入口
│   ├── shared/                       # Console、StepPause、CollectingSink 等
│   └── exercises/                    # Ex01FirstJob 至 Ex14ProductionPatterns
└── src/test/java/playground/
    ├── MiniClusterTestBase.java
    ├── helpers/StreamAssertions.java
    └── exercises/                    # 每个练习一个测试
```

---

## 命令

| 命令 | 描述 |
|------|------|
| `make help` | 显示所有可用目标 |
| `make cluster-start` | 通过 Docker Compose 启动 Flink 集群 |
| `make cluster-stop` | 停止 Flink 集群 |
| `make cluster-status` | 显示集群健康状态（REST API） |
| `make build` | 构建 fat JAR（`./mvnw package`） |
| `make test` | 运行所有 MiniCluster 测试 |
| `make test-coverage` | 运行测试并生成 JaCoCo 覆盖率报告 |
| `make exercise-01` | 在 Docker 集群上运行练习 01 |
| `make exercise-local-01` | 使用 MiniCluster 运行练习 01 |

---

## 设计决策

1. **Java 21 + Flink 2.0** — Java 21 是最新 LTS 版本。Flink 2.0 提供最新的 DataStream API 及统一的 Sink/SinkWriter 接口。
2. **双运行模式** — `--local` 使用进程内 MiniCluster（快速，无需 Docker）；默认使用 Docker Compose 集群（真实 Flink）。
3. **CollectingSink 模式** — 自定义 sink 将记录收集到 `List<T>` 中以进行确定性测试断言，无异步竞态问题。
4. **有界流** — 所有练习使用有限数据集，以确保快速、可重复的结果。
5. **单 Maven 模块** — 每个练习是继承 `ExerciseRunner` 的一个 Java 类，无多模块复杂性。
6. **交互式逐步模式** — `--step` 标志在操作之间暂停，便于学习和检查。
7. **双语文档** — 每个模块都有英文和中文概念文档。
8. **零框架依赖** — 仅使用 Flink、JUnit 5 和 AssertJ，无 Spring、无 Lombok。

---

## CI 流水线

每次推送和 PR 进行双通道策略：

| 通道 | 内容 | 运行时间 |
|------|------|----------|
| **test** | 编译 + MiniCluster 测试（JDK 17 & 21） | ~2 分钟 |
| **integration** | Docker Compose 集群 + `flink run` 所有练习 | ~8 分钟 |

两者必须在合并前全部通过。

---

## 关键 Flink 概念对应

| 概念 | 所在模块 |
|------|----------|
| DataStream API | 01, 02 |
| 键控状态 | 02, 03 |
| 状态后端 | 03, 06 |
| 窗口 | 04, 18, 21 |
| 事件时间 / 水位线 | 05, 24 |
| 检查点 | 06, 23 |
| Kafka 数据源/数据汇 | 07 |
| 文件 I/O | 08, 20 |
| Table API | 09 |
| Flink SQL | 10, 18 |
| ProcessFunction | 11, 24 |
| CEP 模式 | 12 |
| 广播状态 | 13 |
| Savepoints 与指标 | 14, 23 |
| 异步 I/O | 15 |
| 流连接 | 16 |
| JDBC 丰富 | 17 |
| SQL 窗口 TVF | 18 |
| 内置 SQL UDF | 19 |
| JSON/CSV 格式 | 20 |
| 全局窗口与触发器 | 21 |
| 重启策略 | 22 |
| 处理时间语义 | 24 |

---

## 许可证

MIT
