# Exercise 17 — JDBC Connector

## What You'll Learn

- H2 in-memory database for zero-install development
- Direct JDBC lookups within Flink map functions
- Database enrichment patterns

---

## Why This Matters

Flink 2.0 moved JDBC to the Table/SQL API. This exercise demonstrates direct JDBC integration within a map function — a practical pattern for database lookups and enrichment that works in both MiniCluster and Docker environments.

---

## Core Concepts

H2 is an in-memory database that requires no installation. The exercise creates a product catalog, then enriches order streams by looking up product prices via JDBC queries inside a map function.

## Gotchas

- H2 is great for testing — no install needed, works everywhere
- For production, use connection pooling (HikariCP) and proper JDBC drivers
- Blocking calls inside map functions reduce throughput — consider async I/O for production
