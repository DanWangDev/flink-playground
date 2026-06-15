# Exercise 09 — Table API

## What You'll Learn
- StreamTableEnvironment — Table API entry point
- fromDataStream — converting a DataStream to a Table
- SQL queries with GROUP BY on streaming data
- toChangelogStream — converting Table results back to a stream

## Why This Matters
The Table API is a declarative alternative to the DataStream API. Write SQL queries instead of Java transformations for aggregations and filtering.

## Core Concepts
```java
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
Table orders = tEnv.fromDataStream(stream, Schema.newBuilder()
    .column("orderId", DataTypes.STRING())
    .column("customerId", DataTypes.STRING())
    .column("amount", DataTypes.DOUBLE()).build());
tEnv.createTemporaryView("orders", orders);
Table result = tEnv.sqlQuery("SELECT customerId, SUM(amount) FROM orders GROUP BY customerId");
DataStream<Row> output = tEnv.toChangelogStream(result);
```

## Gotchas
- GROUP BY produces update changes — must use toChangelogStream()
- Table planner is provided scope — the cluster provides it
- Do NOT bundle the planner in your fat JAR
