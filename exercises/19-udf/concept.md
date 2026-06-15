# Exercise 19 — UDF (User-Defined Functions)

## What You'll Learn

- Built-in SQL functions: ROUND, arithmetic operators
- Computed columns with expressions
- Tax calculation using SQL expressions

---

## Why This Matters

Flink SQL supports built-in functions like ROUND, FLOOR, and arithmetic operators. These cover many common business logic use cases without requiring custom Java/Scala UDF classes.

---

## Core Concepts

```sql
SELECT orderId, amount,
       ROUND(amount * 0.08, 2) AS tax,
       ROUND(amount * 1.08, 2) AS total
FROM orders WHERE amount > 100;
```

## Gotchas

- Custom UDF classes need createTemporarySystemFunction registration
- Flink 2.x UDF registration can be sensitive in MiniCluster tests
- Built-in functions are always available and more portable across environments
