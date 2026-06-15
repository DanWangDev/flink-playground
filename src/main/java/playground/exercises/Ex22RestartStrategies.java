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

public class Ex22RestartStrategies extends ExerciseRunner {

    public Ex22RestartStrategies() {
        super("22-restart-strategies", "Restart Strategies");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Configure restart strategy via Configuration
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.setString("restart-strategy.type", "fixed-delay");
        config.setInteger("restart-strategy.fixed-delay.attempts", 3);
        config.setString("restart-strategy.fixed-delay.delay", "1s");
        env.configure(config);

        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        CollectingSink<String> sink = new CollectingSink<>();

        orders.keyBy(DataSources.OrderEvent::customerId)
            .process(new KeyedProcessFunction<String, DataSources.OrderEvent, String>() {
                private ValueState<Integer> counter;

                @Override
                public void open(OpenContext ctx) {
                    counter = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("counter", Integer.class));
                }

                @Override
                public void processElement(DataSources.OrderEvent order, Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer count = counter.value();
                    int newCount = (count == null) ? 1 : count + 1;
                    counter.update(newCount);
                    out.collect(String.format("processed %s (#%d)", order.orderId(), newCount));
                }
            }).sinkTo(sink);

        env.execute("Exercise 22 — Restart Strategies");
        return sink.getValues();
    }
}
