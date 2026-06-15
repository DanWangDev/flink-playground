package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

public class Ex21GlobalWindows extends ExerciseRunner {

    public Ex21GlobalWindows() {
        super("21-global-windows", "Global Windows & Custom Triggers");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        CollectingSink<String> sink = new CollectingSink<>();
        orders.keyBy(DataSources.OrderEvent::customerId)
            .window(GlobalWindows.create())
            .trigger(PurgingTrigger.of(CountTrigger.of(2)))  // Fire every 2 elements, purge after
            .process(new ProcessWindowFunction<DataSources.OrderEvent, String,
                         String, GlobalWindow>() {
                @Override
                public void process(String key, Context ctx,
                                    Iterable<DataSources.OrderEvent> elements,
                                    Collector<String> out) {
                    double sum = 0;
                    for (var e : elements) sum += e.amount();
                    out.collect(String.format("customer=%s batch-total=%.2f", key, sum));
                }
            }).sinkTo(sink);

        env.execute("Exercise 21 — Global Windows");
        return sink.getValues();
    }
}
