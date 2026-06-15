# 练习 07 — Kafka 连接器

## 你将学到

- **KafkaSource** — 从 Kafka 主题读取，支持精确一次
- **KafkaSink** — 写入 Kafka，具有交付保证
- 实际 Kafka 与内存回退的双模式设计

## 关键 API

```java
// 读取
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setTopics("input-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

// 写入
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("localhost:9092")
    .setRecordSerializer(...)
    .build();
```

## 注意事项
- 需要运行中的 Kafka 集群（Docker Compose）
- MiniCluster 测试使用内存回退
- 偏移量通过检查点管理以实现精确一次
