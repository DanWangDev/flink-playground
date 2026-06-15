# Exercise 08 — File Connectors

## What You'll Learn

- FileSource — reading files as a bounded or continuous stream
- TextLineInputFormat — line-by-line text reading
- FileSink — writing streaming results to files
- Docker-compatible file paths for distributed execution

---

## Why This Matters

File connectors bridge batch and stream processing. They let you read existing files as streams and write results to persistent storage for downstream consumption.

---

## Core Concepts

- FileSource.forRecordStreamFormat reads files line by line
- FileSink.forRowFormat writes text rows to output files
- Rolling policies control when files are finalized (size-based or checkpoint-based)

### Docker Compatibility

Write to the data/ directory which is mounted as a shared volume in Docker Compose. Files created on the CI runner are accessible inside Flink containers.

## Gotchas

- File paths must be accessible from all Flink task managers
- Use shared volumes (like data/) for Docker deployments
- File.createTempFile() creates local-only files — avoid in distributed setups
