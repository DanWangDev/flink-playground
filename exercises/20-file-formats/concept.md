# Exercise 20 — File Formats (JSON/CSV)

## What You'll Learn

- CSV reading with TextLineInputFormat
- JSON formatting for structured output
- FileSink with rolling policies

---

## Why This Matters

Most data pipelines involve format conversion — CSV to JSON, JSON to Parquet. This exercise demonstrates reading CSV data and formatting it as JSON for downstream consumption.

---

## Core Concepts

- TextLineInputFormat reads file lines as strings
- Manual CSV parsing with String.split() is simple but effective
- JSON construction with String.format() produces structured output
- FileSink writes formatted output with configurable rolling policies

## Gotchas

- File paths must be accessible from all task managers
- Use data/ directory for Docker-compatible file I/O
- flink-csv and flink-json dependencies are available for advanced format handling
