# 练习 12 — CEP（复杂事件处理）

## 你将学到
- Pattern API — 定义要匹配的事件序列
- begin/next — 事件之间的严格连续性
- PatternProcessFunction — 处理匹配的事件序列

## 注意事项
- 模式按键评估 — 在 CEP.pattern() 前使用 keyBy()
- 对于时间限制的模式，添加 .within(Time.minutes(5))
