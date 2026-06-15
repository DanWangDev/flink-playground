# Exercise 18 — Window TVFs in SQL

## What You'll Learn

- TUMBLE window table-valued function in Flink SQL
- PROCTIME() for processing-time windows
- DESCRIPTOR syntax for window specification

---

## Why This Matters

Window TVFs (Table-Valued Functions) are the modern SQL windowing API in Flink. They replace legacy GROUP BY window syntax with clearer, more flexible window definitions.

---

## Core Concepts

```sql
SELECT window_start, customerId, SUM(amount) AS total
FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(proctime), INTERVAL '10' SECONDS))
GROUP BY window_start, customerId;
```

TUMBLE creates fixed-size, non-overlapping windows. Use PROCTIME() when the source table has no event-time column.

## Gotchas

- PROCTIME() for tables without event-time columns
- TUMBLE windows are fixed-size and non-overlapping
- Window TVFs require Flink 1.13+
