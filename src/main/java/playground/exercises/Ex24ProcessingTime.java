package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Ex24ProcessingTime extends ExerciseRunner {

    public Ex24ProcessingTime() {
        super("24-processing-time", "Processing Time Semantics");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Part 1: Processing-time windows (wall-clock based)
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        CollectingSink<String> procSink = new CollectingSink<>();
        orders.keyBy(DataSources.OrderEvent::customerId)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
            .process(new ProcessWindowFunction<DataSources.OrderEvent, String,
                         String, TimeWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.OrderEvent> elements,
                                    Collector<String> out) {
                    double sum = 0;
                    for (var e : elements) sum += e.amount();
                    out.collect(String.format("PROC-TIME customer=%s total=%.2f", key, sum));
                }
            }).sinkTo(procSink);

        env.execute("Exercise 24 — Processing Time");
        List<String> results = new ArrayList<>(procSink.getValues());

        log.keyValue("Processing-time windows", String.valueOf(results.size()));
        results.forEach(r -> log.output("  " + r));
        return results;
    }
}
