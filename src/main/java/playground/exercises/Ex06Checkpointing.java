package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 06 — Checkpointing & Fault Tolerance
 */
public class Ex06Checkpointing extends ExerciseRunner {

    public Ex06Checkpointing() {
        super("06-checkpointing", "Checkpointing & Fault Tolerance");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Step 1: Configure checkpointing ──
        log.section("Step 1: Configuring Checkpointing");
        log.concept(
            "Checkpointing snapshots the entire state of the job at regular intervals. " +
            "If a failure occurs, Flink restores from the last successful checkpoint. " +
            "Key settings: interval, mode (EXACTLY_ONCE/AT_LEAST_ONCE), timeout."
        );

        env.enableCheckpointing(5000);  // checkpoint every 5 seconds
        env.getCheckpointConfig().setCheckpointingMode(
            org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        log.info("Checkpointing enabled: 5s interval, EXACTLY_ONCE, file backend");

        // ── Step 2: Stateful processing with checkpoint recovery ──
        log.section("Step 2: Stateful Processing with Checkpoints");
        log.concept(
            "State declared via ValueState, ListState, or MapState is automatically " +
            "checkpointed. On recovery, Flink restores the exact state from the last " +
            "successful checkpoint. No code changes needed — it's transparent."
        );

        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        CollectingSink<String> sink = new CollectingSink<>();

        orders
            .keyBy(DataSources.OrderEvent::customerId)
            .process(new KeyedProcessFunction<String, DataSources.OrderEvent, String>() {
                private ValueState<Double> total;

                @Override
                public void open(OpenContext ctx) {
                    total = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("running-total", Double.class));
                }

                @Override
                public void processElement(DataSources.OrderEvent order, Context ctx,
                                           Collector<String> out) throws Exception {
                    Double current = total.value();
                    double newTotal = (current == null ? 0 : current) + order.amount();
                    total.update(newTotal);
                    out.collect(String.format("customer=%s runningTotal=%.2f order=%s",
                        order.customerId(), newTotal, order.orderId()));
                }
            })
            .name("checkpointed-counter")
            .sinkTo(sink);

        env.execute("Exercise 06 — Checkpointing");
        List<String> results = sink.getValues();

        log.success("Checkpointed results (" + results.size() + " records):");
        results.forEach(r -> log.output("  " + r));
        return results;
    }
}
