package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.time.Duration;
import java.util.List;

/**
 * Exercise 05 — Event Time & Watermarks
 */
public class Ex05EventTime extends ExerciseRunner {

    private static final OutputTag<DataSources.SensorReading> LATE_DATA =
        new OutputTag<DataSources.SensorReading>("late-data") {};

    public Ex05EventTime() {
        super("05-event-time", "Event Time & Watermarks");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Step 1: Event time with watermarks ──
        log.section("Step 1: Event Time with Watermarks");
        log.concept(
            "Event time processes events based on their embedded timestamp, not " +
            "wall-clock time. Watermarks declare 'no events older than T will arrive'. " +
            "They allow Flink to know when windows are complete and can be emitted."
        );

        DataStream<DataSources.SensorReading> sensors = DataSources.sensors(env);
        CollectingSink<String> windowSink = new CollectingSink<>();

        sensors
            .keyBy(DataSources.SensorReading::sensorId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
            .process(new ProcessWindowFunction<DataSources.SensorReading, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.SensorReading> elements,
                                    Collector<String> out) {
                    double avg = 0;
                    int count = 0;
                    for (var e : elements) { avg += e.temperature(); count++; }
                    avg /= count;
                    out.collect(String.format(
                        "event-time window=[%d-%d] sensor=%s avg=%.1f count=%d",
                        ctx.window().getStart(), ctx.window().getEnd(), key, avg, count));
                }
            })
            .name("event-time-window")
            .sinkTo(windowSink);

        env.execute("Exercise 05 — Event Time Windows");
        List<String> windowResults = windowSink.getValues();
        log.success("Event time window results (" + windowResults.size() + "):");
        windowResults.forEach(r -> log.output("  " + r));

        // ── Step 2: Handling late data with side outputs ──
        log.section("Step 2: Handling Late Data with Side Outputs");
        log.concept(
            "Events arriving after the watermark may be dropped or redirected to a " +
            "side output. Side outputs let you capture late data for separate processing " +
            "or logging."
        );

        var sensors2 = DataSources.sensors(env);
        CollectingSink<String> lateSink = new CollectingSink<>();

        var mainStream = sensors2
            .keyBy(DataSources.SensorReading::sensorId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
            .sideOutputLateData(LATE_DATA)
            .process(new ProcessWindowFunction<DataSources.SensorReading, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.SensorReading> elements,
                                    Collector<String> out) {
                    double avg = 0;
                    int count = 0;
                    for (var e : elements) { avg += e.temperature(); count++; }
                    avg /= count;
                    out.collect(String.format("main sensor=%s avg=%.1f count=%d", key, avg, count));
                }
            });

        // Capture late data via side output
        mainStream.getSideOutput(LATE_DATA)
            .map(r -> "LATE: " + r.toString())
            .returns(String.class)
            .sinkTo(lateSink);

        // Sink main output
        mainStream.sinkTo(new CollectingSink<>()).name("main-sink");

        env.execute("Exercise 05 — Late Data Handling");

        List<String> allResults = new java.util.ArrayList<>(windowResults);
        allResults.addAll(lateSink.getValues());
        log.success("Late data records: " + lateSink.getValues().size());
        lateSink.getValues().forEach(r -> log.output("  " + r));

        return allResults;
    }
}
