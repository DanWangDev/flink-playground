package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

public class Ex23Savepoints extends ExerciseRunner {

    public Ex23Savepoints() {
        super("23-savepoints", "Savepoints & Rescaling");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        CollectingSink<String> sink = new CollectingSink<>();

        orders.keyBy(DataSources.OrderEvent::customerId)
            .process(new KeyedProcessFunction<String, DataSources.OrderEvent, String>() {
                private ValueState<Double> runningTotal;

                @Override
                public void open(OpenContext ctx) {
                    runningTotal = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("runningTotal", Double.class));
                }

                @Override
                public void processElement(DataSources.OrderEvent order, Context ctx,
                                           Collector<String> out) throws Exception {
                    Double current = runningTotal.value();
                    double newTotal = (current == null ? 0 : current) + order.amount();
                    runningTotal.update(newTotal);
                    out.collect(String.format("customer=%s runningTotal=%.2f (state persists across savepoints)",
                        order.customerId(), newTotal));
                }
            }).sinkTo(sink);

        env.execute("Exercise 23 — Savepoints");
        return sink.getValues();
    }
}
