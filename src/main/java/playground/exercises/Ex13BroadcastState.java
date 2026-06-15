package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

public class Ex13BroadcastState extends ExerciseRunner {

    public Ex13BroadcastState() {
        super("13-broadcast-state", "Broadcast State");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Main stream: orders
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        // Broadcast stream: discount rules
        DataStream<String> rules = env.fromData(
            "cust-a:10",  // 10% discount for cust-a
            "cust-b:5"    // 5% discount for cust-b
        );

        MapStateDescriptor<String, Double> ruleDescriptor = new MapStateDescriptor<>(
            "discount-rules", String.class, Double.class);
        BroadcastStream<String> broadcastRules = rules.broadcast(ruleDescriptor);

        CollectingSink<String> sink = new CollectingSink<>();
        orders.connect(broadcastRules)
            .process(new BroadcastProcessFunction<DataSources.OrderEvent, String, String>() {
                @Override
                public void processElement(DataSources.OrderEvent order, ReadOnlyContext ctx,
                                           Collector<String> out) throws Exception {
                    Double discount = ctx.getBroadcastState(ruleDescriptor).get(order.customerId());
                    double discounted = discount != null
                        ? order.amount() * (1 - discount / 100)
                        : order.amount();
                    out.collect(String.format("%s: $%.2f -> $%.2f (%.0f%% off)",
                        order.orderId(), order.amount(), discounted,
                        discount != null ? discount : 0));
                }

                @Override
                public void processBroadcastElement(String rule, Context ctx,
                                                    Collector<String> out) throws Exception {
                    String[] parts = rule.split(":");
                    ctx.getBroadcastState(ruleDescriptor).put(parts[0], Double.parseDouble(parts[1]));
                }
            }).sinkTo(sink);

        env.execute("Exercise 13 — Broadcast State");
        return sink.getValues();
    }
}
