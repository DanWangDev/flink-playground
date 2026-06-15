# 练习 21 — 全局窗口与自定义触发器

## 你将学到

- GlobalWindow — 默认永不结束的窗口
- CountTrigger — 累积 N 个元素后触发
- PurgingTrigger — 每次触发后清除窗口状态
- 组合触发器实现自定义窗口行为

---

## 核心概念

```java
stream.keyBy(...)
    .window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(CountTrigger.of(2)))
    .process(...);
```

- GlobalWindow 为每个键创建单个窗口
- CountTrigger 每 N 个元素触发一次
- PurgingTrigger 触发后清除所有元素（无状态窗口）
- 不清除会导致元素无限累积

## 注意事项

- 没有触发器的 GlobalWindow 永远不会触发 — 始终设置触发器
- 没有 PurgingTrigger 状态会无限增长（内存泄漏）
- 组合触发器实现复杂策略：时间或计数、计数和大小等
