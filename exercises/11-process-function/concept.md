# Exercise 11 — ProcessFunction

## What You'll Learn
- KeyedProcessFunction — the most flexible stream operator
- Timers — schedule future actions per key
- Side outputs — emit data to multiple output streams
- Context API — access timestamps, watermarks, and state

## Core Concepts
ProcessFunction gives full control: schedule timers, emit side outputs,
and access low-level context like watermarks and timestamps.

## Gotchas
- Timers fire when watermark passes trigger time
- Side outputs declared as OutputTag with type information
- ProcessFunction state is checkpointed — survives failures
