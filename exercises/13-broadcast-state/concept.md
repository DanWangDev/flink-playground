# Exercise 13 — Broadcast State

## What You'll Learn
- BroadcastStream — sending data to all parallel instances
- BroadcastProcessFunction — processing main stream with broadcast state
- Dynamic rule application — update rules without restarting

## Core Concepts
Broadcast state is Flink's pattern for dynamic configuration.
Rules or reference data can be updated at runtime and immediately
applied to all processing — no restart needed.

Use MapStateDescriptor to declare broadcast state schema.
Connect a NON-keyed stream with the broadcast stream.

## Gotchas
- Broadcast state is replicated to ALL parallel instances — keep it small
- Must be used on a NON-keyed stream (do NOT keyBy before connect)
- Each parallel instance gets its own copy of broadcast state
