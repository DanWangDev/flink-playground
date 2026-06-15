# Exercise 15 — Async I/O

## What You'll Learn

- AsyncDataStream.unorderedWait for non-blocking external calls
- CompletableFuture for async result handling
- Timeout and capacity configuration
- Ordered vs unordered processing

---

## Why This Matters

Production Flink jobs call external services (databases, APIs). Blocking calls kill throughput. Async I/O lets you call external services without blocking the stream processing thread.

---

## Core Concepts

AsyncDataStream.unorderedWait takes four parameters:
- The input stream
- An AsyncFunction that performs the async call
- A timeout (max wait per request)
- A capacity (max concurrent requests)

unorderedWait emits results as they arrive (faster). orderedWait preserves input order.

## Gotchas

- Async function runs on a separate thread pool — don't block the main operator thread
- Ordering guarantee is per-input-partition, not global
- Always set a reasonable timeout — hung external calls block capacity slots
