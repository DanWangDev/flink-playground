package playground.exercises;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 02 — Keyed Streams
 * ===========================
 * Learn keyBy, reduce, aggregate, and RichMapFunction for parallel processing
 * by key groups.
 */
public class Ex02KeyedStreams extends ExerciseRunner {

    public Ex02KeyedStreams() {
        super("02-keyed-streams", "Keyed Streams");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        env.setParallelism(2);

        // ── Step 1: Understanding keyBy ──
        log.section("Step 1: Partitioning with keyBy");
        log.concept(
            "keyBy partitions the stream by a key selector, sending all records with " +
            "the same key to the same parallel subtask. This is the foundation for " +
            "stateful operations like reduce, aggregate, and windowing."
        );

        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        KeyedStream<DataSources.OrderEvent, String> byCustomer = orders
            .keyBy(order -> order.customerId());

        log.info("Orders partitioned by customerId into " + env.getParallelism() + " subtasks");
        step.pause("Stream keyed by customerId");

        // ── Step 2: Summing with reduce ──
        log.section("Step 2: Aggregating with reduce");
        log.concept(
            "reduce combines the current value with the previous reduced value for each key. " +
            "It maintains running state per key. Here we sum order amounts per customer."
        );

        CollectingSink<String> reduceSink = new CollectingSink<>();

        byCustomer
            .reduce((o1, o2) -> new DataSources.OrderEvent(
                o1.orderId() + "+" + o2.orderId(),
                o1.customerId(),
                o1.amount() + o2.amount()
            ))
            .name("reduce-amounts")
            .map(o -> String.format("customer=%s total=%.2f", o.customerId(), o.amount()))
            .returns(String.class)
            .sinkTo(reduceSink);

        env.execute("Exercise 02 — Keyed Streams (reduce)");
        List<String> reduceResults = reduceSink.getValues();

        log.success("Reduce results:");
        reduceResults.forEach(r -> log.output("  " + r));

        // ── Step 3: aggregating with RichMapFunction ──
        log.section("Step 3: Per-key Counting with RichMapFunction");
        log.concept(
            "RichMapFunction gives access to RuntimeContext, which provides the current " +
            "subtask index. Combined with keyBy, each key group processes independently. " +
            "Here we count orders per customer using a simple counter."
        );

        var orders2 = DataSources.orders(env);
        CollectingSink<String> countSink = new CollectingSink<>();

        orders2
            .keyBy(order -> order.customerId())
            .map(new RichMapFunction<DataSources.OrderEvent, String>() {
                private int count = 0;

                @Override
                public void open(OpenContext openContext) {
                    count = 0;
                }

                @Override
                public String map(DataSources.OrderEvent order) {
                    count++;
                    return String.format("customer=%s orderCount=%d order=%s",
                        order.customerId(), count, order.orderId());
                }
            })
            .returns(String.class)
            .name("per-key-counter")
            .sinkTo(countSink);

        env.execute("Exercise 02 — Keyed Streams (per-key count)");
        List<String> countResults = countSink.getValues();

        log.success("Per-key count results:");
        countResults.forEach(r -> log.output("  " + r));

        // ── Step 4: Tuple-based aggregation ──
        log.section("Step 4: Using Tuples for Structured Aggregation");
        log.concept(
            "Flink's Tuple types (Tuple2, Tuple3, ...) are convenient for intermediate " +
            "aggregations. Here we emit (customerId, amount) tuples and sum amounts per key."
        );

        var orders3 = DataSources.orders(env);
        CollectingSink<String> tupleSink = new CollectingSink<>();

        orders3
            .map(order -> Tuple2.of(order.customerId(), order.amount()))
            .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.DOUBLE))
            .keyBy(tuple -> tuple.f0)
            .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
            .name("tuple-reduce")
            .map(t -> String.format("customer=%s totalSpend=%.2f", t.f0, t.f1))
            .returns(String.class)
            .sinkTo(tupleSink);

        env.execute("Exercise 02 — Keyed Streams (tuple aggregation)");
        List<String> tupleResults = tupleSink.getValues();

        log.success("Tuple aggregation results:");
        tupleResults.forEach(r -> log.output("  " + r));

        // ── Summary ──
        log.separator();
        log.keyValue("Total reduce results", String.valueOf(reduceResults.size()));
        log.keyValue("Total count results", String.valueOf(countResults.size()));
        log.keyValue("Total tuple aggregation results", String.valueOf(tupleResults.size()));

        // Return all results for test assertions
        java.util.List<String> allResults = new java.util.ArrayList<>();
        allResults.addAll(reduceResults);
        allResults.addAll(countResults);
        allResults.addAll(tupleResults);
        return allResults;
    }
}
