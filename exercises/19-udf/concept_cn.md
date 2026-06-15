# 练习 19 — UDF（用户自定义函数）

## 你将学到

- 内置 SQL 函数：ROUND、算术运算
- 使用表达式计算列
- 使用 SQL 表达式进行税务计算

---

## 核心概念

```sql
SELECT orderId, amount,
       ROUND(amount * 0.08, 2) AS tax,
       ROUND(amount * 1.08, 2) AS total
FROM orders WHERE amount > 100;
```

## 注意事项

- 自定义 UDF 类需要 createTemporarySystemFunction 注册
- Flink 2.0 UDF 注册在 MiniCluster 测试中可能敏感
- 内置函数始终可用且在不同环境中更具可移植性
