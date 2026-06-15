# 练习 24 — 处理时间语义

## 你将学到

- 处理时间与事件时间的比较
- 通过 ProcessFunction 访问时间戳和水位线
- 确定性与非确定性时间语义

---

## 核心概念

```java
stream.process(new ProcessFunction<Event, String>() {
    void processElement(Event e, Context ctx, Collector<String> out) {
        long procTime = ctx.timerService().currentProcessingTime();
        long wm = ctx.timerService().currentWatermark();
    }
});
```

- 处理时间：墙上时钟时间，非确定性，始终可用
- 事件时间：嵌入在数据中，确定性，需要水位线
- 水位线：跟踪事件时间进度，未设置水位线策略时为 null

## 注意事项

- 处理时间结果在不同运行间不同 — 不适合测试
- 使用固定时间戳的事件时间给出可重现的结果
- 对于确定性测试，使用 `WatermarkStrategy.forBoundedOutOfOrderness`
