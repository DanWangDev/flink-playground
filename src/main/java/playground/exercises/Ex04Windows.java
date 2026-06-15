package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import java.time.Duration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 04 — Windows
 * =====================
 * Learn tumbling, sliding, and session windows with ProcessWindowFunction.
 */
public class Ex04Windows extends ExerciseRunner {

    public Ex04Windows() {
        super("04-windows", "Windows");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Step 1: Tumbling windows ──
        log.section("Step 1: Tumbling Windows — Fixed-Size, Non-Overlapping");
        log.concept(
            "Tumbling windows are fixed-size, non-overlapping buckets. Each event " +
            "belongs to exactly one window. When the window closes, the aggregate " +
            "is emitted. Windows are defined by their size (e.g., 5 seconds)."
        );

        DataStream<DataSources.SensorReading> sensors = DataSources.sensors(env);
        CollectingSink<String> tumblingSink = new CollectingSink<>();

        sensors
            .keyBy(DataSources.SensorReading::sensorId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
            .process(new ProcessWindowFunction<DataSources.SensorReading, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.SensorReading> elements,
                                    Collector<String> out) {
                    double sum = 0;
                    int count = 0;
                    for (var e : elements) { sum += e.temperature(); count++; }
                    double avg = sum / count;
                    out.collect(String.format(
                        "tumbling window=[%d-%d] sensor=%s avg=%.1f count=%d",
                        ctx.window().getStart(), ctx.window().getEnd(),
                        key, avg, count));
                }
            })
            .name("tumbling-window")
            .sinkTo(tumblingSink);

        env.execute("Exercise 04 — Tumbling Windows");
        List<String> tumblingResults = tumblingSink.getValues();
        log.success("Tumbling window results (" + tumblingResults.size() + "):");
        tumblingResults.forEach(r -> log.output("  " + r));

        // ── Step 2: Sliding windows ──
        log.section("Step 2: Sliding Windows — Fixed-Size, Overlapping");
        log.concept(
            "Sliding windows slide by a step smaller than the window size, creating " +
            "overlap. An event can belong to multiple windows. Parameters: " +
            "window size and slide interval."
        );

        var sensors2 = DataSources.sensors(env);
        CollectingSink<String> slidingSink = new CollectingSink<>();

        sensors2
            .keyBy(DataSources.SensorReading::sensorId)
            .window(SlidingEventTimeWindows.of(Duration.ofSeconds(4), Duration.ofSeconds(2)))
            .process(new ProcessWindowFunction<DataSources.SensorReading, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.SensorReading> elements,
                                    Collector<String> out) {
                    double sum = 0;
                    int count = 0;
                    for (var e : elements) { sum += e.temperature(); count++; }
                    double avg = sum / count;
                    out.collect(String.format(
                        "sliding window=[%d-%d] sensor=%s avg=%.1f count=%d",
                        ctx.window().getStart(), ctx.window().getEnd(),
                        key, avg, count));
                }
            })
            .name("sliding-window")
            .sinkTo(slidingSink);

        env.execute("Exercise 04 — Sliding Windows");
        List<String> slidingResults = slidingSink.getValues();
        log.success("Sliding window results (" + slidingResults.size() + "):");
        slidingResults.forEach(r -> log.output("  " + r));

        // ── Step 3: Session windows ──
        log.section("Step 3: Session Windows — Activity-Based, Variable-Size");
        log.concept(
            "Session windows group events separated by a gap of inactivity. " +
            "Each session can be a different size. Useful for user sessions, " +
            "where you want to group events that happen close together."
        );

        var sensors3 = DataSources.sensors(env);
        CollectingSink<String> sessionSink = new CollectingSink<>();

        sensors3
            .keyBy(DataSources.SensorReading::sensorId)
            .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(2)))
            .process(new ProcessWindowFunction<DataSources.SensorReading, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.SensorReading> elements,
                                    Collector<String> out) {
                    double max = Double.MIN_VALUE;
                    int count = 0;
                    for (var e : elements) {
                        max = Math.max(max, e.temperature());
                        count++;
                    }
                    out.collect(String.format(
                        "session window=[%d-%d] sensor=%s max=%.1f count=%d",
                        ctx.window().getStart(), ctx.window().getEnd(),
                        key, max, count));
                }
            })
            .name("session-window")
            .sinkTo(sessionSink);

        env.execute("Exercise 04 — Session Windows");
        List<String> sessionResults = sessionSink.getValues();
        log.success("Session window results (" + sessionResults.size() + "):");
        sessionResults.forEach(r -> log.output("  " + r));

        // ── Summary ──
        List<String> all = new java.util.ArrayList<>();
        all.addAll(tumblingResults);
        all.addAll(slidingResults);
        all.addAll(sessionResults);
        return all;
    }
}
