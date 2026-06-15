# 练习 10 — Flink SQL

## 你将学到
- DDL — 从 DataStream 创建临时视图
- 聚合 — 流上的 COUNT, SUM, AVG
- 变更日志流 — 理解 INSERT/UPDATE/DELETE 行

## 注意事项
- 无窗口的 GROUP BY 产生更新结果 — 使用 toChangelogStream()
- Planner 是 provided 作用域
