# 练习 09 — Table API

## 你将学到
- StreamTableEnvironment — Table API 入口
- fromDataStream — 将 DataStream 转换为 Table
- 流数据上的 GROUP BY SQL 查询
- toChangelogStream — 将 Table 结果转换回流

## 注意事项
- GROUP BY 产生更新变更 — 必须使用 toChangelogStream()
- Table Planner 是 provided 作用域 — 集群提供它
