# Exercise 07 — Kafka Connector

## What You'll Learn

- **KafkaSource** — reading from Kafka topics with exactly-once support
- **KafkaSink** — writing to Kafka with delivery guarantees
- **Serialization** — SimpleStringSchema, JSON, Avro
- When to use Kafka as a streaming backbone

---

## Key APIs

```java
// Reading
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("input-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

// Writing
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("output-topic")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
    .build();
stream.sinkTo(sink);
```

## Gotchas
- KafkaSource requires a running Kafka cluster (Docker Compose in this project)
- MiniCluster tests use an in-memory fallback
- Offsets are managed via checkpointing for exactly-once
