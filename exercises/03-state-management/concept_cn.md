# 练习 03 — 状态管理

## 你将学到

- **ValueState** — 每个键的单个值（计数器、最后见到的值）
- **ListState** — 每个键的累积列表（会话事件、历史记录）
- **MapState** — 每个键的键值对（每个类别的计数）
- Flink 如何在幕后管理状态

---

## 为什么这很重要

状态将无状态流处理器转变为有状态流处理器。没有状态，每个事件都是独立处理的。有了状态，你可以计数、累积、连接和记忆 — 这是实时分析、欺诈检测和会话分析的基础。

---

## 状态类型

| 类型 | 结构 | 用例 |
|------|------|------|
| `ValueState<V>` | 每个键的单个值 | 计数器、最后事件、标志 |
| `ListState<E>` | 每个键的有序列表 | 会话事件、历史记录 |
| `MapState<K,V>` | 每个键的 HashMap | 类别计数、缓存 |
| `ReducingState<V>` | 具有 reduce 的单个值 | 运行聚合 |

所有这些都从 `RichFunction` 或 `ProcessFunction` 内部的 `RuntimeContext` 获取。

---

## 工作原理

```
keyBy(userId) → ValueState<Integer>
  user-1: [value=3]
  user-2: [value=1]
  user-3: [value=2]

keyBy(userId) → ListState<String>
  user-1: ["page-a", "page-c", "page-b"]
  user-2: ["page-b", "page-a", "page-c"]

keyBy(userId) → MapState<String,Integer>
  user-1: {"page-a": 1, "page-b": 2, "page-c": 1}
```

Flink 将状态存储在配置的状态后端中：
- **HashMapStateBackend** — 内存中，快速，用于测试/小状态
- **EmbeddedRocksDBStateBackend** — 磁盘上，用于大状态（TB+）

状态通过检查点进行容错 — 恢复时，Flink 从最后一个检查点恢复精确状态。

---

## 你将练习

1. 使用 `ValueState` 计算每个用户的点击次数
2. 使用 `ListState` 累积访问过的页面
3. 使用 `MapState` 计算每个用户每个页面的访问次数

---

## 常见陷阱

1. **状态是键控的** — 状态只能在 `keyBy` + `KeyedProcessFunction` 内部访问。没有键控就无法访问状态。

2. **状态描述符** — 始终在 `open()` 中声明状态，而不是内联。描述符告诉 Flink 名称和类型。

3. **Null 处理** — 如果从未设置，`ValueState.value()` 返回 `null`。使用前始终检查 null。

4. **状态 TTL** — 配置 `StateTtlConfig` 以自动清理过期状态。没有 TTL，状态会无限增长。

5. **RocksDB 序列化** — 使用 RocksDB 时，每次状态访问都会从磁盘读取/写入。批量读取以获得更好的性能。
