# Exercise 10 — Flink SQL

## What You'll Learn
- DDL — creating temporary views from DataStreams
- Aggregations — COUNT, SUM, AVG on streams
- Changelog streams — understanding INSERT/UPDATE/DELETE rows

## Core Concepts
```sql
CREATE TEMPORARY VIEW orders AS SELECT * FROM my_stream;
SELECT customerId, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders GROUP BY customerId;
```

## Gotchas
- GROUP BY without window produces updating results — use toChangelogStream()
- The planner is provided scope — don't bundle in fat JAR
