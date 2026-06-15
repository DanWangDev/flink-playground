# 练习 02 — 键控流

## 你将学到

- 如何使用 `keyBy` 对流进行分区以进行并行处理
- 如何使用 `reduce` 进行每个键的运行聚合
- 如何使用 `RichMapFunction` 进行按键状态管理
- 如何使用 Flink 元组处理结构化中间结果

---

## 为什么这很重要

`keyBy` 是状态流处理的门户。没有它，每个操作符都会独立处理每个记录。有了它，共享相同键的所有记录都会落在同一个并行子任务上，从而实现按键聚合、计数，最终实现窗口和状态管理。

---

## 核心概念

### keyBy

```
keyBy 之前：
  子任务 0：[cust-a, cust-b, cust-c, cust-a, cust-b, ...]
  子任务 1：[cust-c, cust-a, cust-b, cust-c, cust-a, ...]

keyBy(customerId) 之后：
  子任务 0：[cust-a, cust-a, cust-a, cust-a]   ← 所有 cust-a 记录
  子任务 1：[cust-b, cust-b, cust-b, cust-c, cust-c, cust-c]  ← 所有 cust-b + cust-c
```

键选择器：
```java
// Lambda 键选择器（最常用）
stream.keyBy(order -> order.customerId())

// 基于字段的选择器（用于元组类型）
stream.keyBy(value -> value.f0)
```

### reduce

`reduce` 将当前元素与之前的归约值组合：

```java
stream.keyBy(o -> o.customerId())
      .reduce((acc, next) -> new Order(acc.id(), acc.customer(), acc.amount() + next.amount()));
```

状态：Flink 维护每个键的最新归约值。恢复时，只恢复最终值。

### RichMapFunction

`RichMapFunction` 提供对 `RuntimeContext`（子任务索引、并行度、指标）的访问。适用于每个实例的计数器和初始化：

```java
stream.keyBy(o -> o.customerId())
      .map(new RichMapFunction<Order, String>() {
          private int count;

          @Override
          public void open(Configuration config) {
              count = 0;  // 每个子任务初始化
          }

          @Override
          public String map(Order order) {
              count++;
              return String.format("%s: 订单 #%d", order.customerId(), count);
          }
      });
```

### Flink 元组

适用于中间结果而不需要创建自定义 POJO：

```java
Tuple2<String, Double> pair = Tuple2.of("cust-a", 100.0);
String id = pair.f0;   // = "cust-a"
Double amt = pair.f1;  // = 100.0
```

---

## 关键概念

| 概念 | 描述 |
|------|------|
| keyBy | 通过键选择器进行逻辑分区 |
| reduce | 运行折叠：(acc, next) → acc |
| RichMapFunction | 带生命周期（open、close）和 RuntimeContext 的 Map |
| Tuple2 | 轻量级 2 字段容器 |
| 并行度 | 处理流的并行子任务数量 |

---

## 你将练习

1. 使用 `keyBy` 按 `customerId` 对订单事件进行分区
2. 使用 `reduce` 对每个客户的订单金额求和
3. 使用 `RichMapFunction` 对每个客户的订单计数
4. 使用 Tuple2 + keyBy + reduce 聚合结构化数据

---

## 常见陷阱

1. **keyBy 不会负载均衡** — 如果一个键包含 90% 的数据，该子任务将成为瓶颈（"热键"问题）。

2. **Lambda 类型擦除** — 在 keyBy 或 reduce 中使用 lambda 时，Flink 可能需要通过 `.returns()` 显式类型提示。

3. **reduce 是有状态的** — 每个键的累积值存储在 Flink 的状态后端中。简单的 reduce 不需要额外的状态声明。

4. **RichFunction.open() vs 构造函数** — 在 `open()` 中而不是构造函数中初始化每个子任务的资源。`open()` 在每个并行实例上运行。

5. **RichMapFunction 状态不具有容错性** — 与 `ValueState` 不同，普通实例字段（`private int count`）不会被检查点保存。它们会在故障时重置。
