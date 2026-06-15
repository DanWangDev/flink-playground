# 练习 18 — Window TVFs in SQL

## 你将学到

- Flink SQL 中的 TUMBLE 窗口表值函数
- PROCTIME() 处理时间窗口
- DESCRIPTOR 窗口规范语法

---

## 核心概念

```sql
SELECT window_start, customerId, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(proctime), INTERVAL '10' SECONDS))
GROUP BY window_start, customerId;
```

TUMBLE 创建固定大小、不重叠的窗口。当源表没有事件时间列时使用 PROCTIME()。

## 注意事项

- 无事件时间列时使用 PROCTIME()
- TUMBLE 窗口固定大小不重叠
- Window TVFs 需要 Flink 1.13+
