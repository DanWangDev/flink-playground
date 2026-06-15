package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

public class Ex14ProductionPatterns extends ExerciseRunner {

    public Ex14ProductionPatterns() {
        super("14-production-patterns", "Production Patterns");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        CollectingSink<String> sink = new CollectingSink<>();
        orders.keyBy(DataSources.OrderEvent::customerId)
            .process(new KeyedProcessFunction<String, DataSources.OrderEvent, String>() {
                private ValueState<Double> total;
                private Counter eventCounter;
                private long lastGaugeValue = 0;

                @Override
                public void open(OpenContext ctx) {
                    total = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("total", Double.class));
                    eventCounter = getRuntimeContext().getMetricGroup().counter("events");
                    getRuntimeContext().getMetricGroup().gauge("runningTotal",
                        (Gauge<Long>) () -> lastGaugeValue);
                }

                @Override
                public void processElement(DataSources.OrderEvent order, Context ctx,
                                           Collector<String> out) throws Exception {
                    eventCounter.inc();
                    Double current = total.value();
                    double newTotal = (current == null ? 0 : current) + order.amount();
                    total.update(newTotal);
                    lastGaugeValue = (long) newTotal;
                    out.collect(String.format("customer=%s total=$%.2f metrics=[events=%d|total=%d]",
                        order.customerId(), newTotal, eventCounter.getCount(), lastGaugeValue));
                }
            }).sinkTo(sink);

        env.execute("Exercise 14 — Production Patterns");
        return sink.getValues();
    }
}
