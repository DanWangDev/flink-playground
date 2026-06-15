package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Ex16StreamJoins extends ExerciseRunner {

    public Ex16StreamJoins() {
        super("16-stream-joins", "Multi-Stream Joins");
    }

    public record Shipment(String orderId, String status, long timestamp) {}

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Stream 1: Orders with deterministic timestamps
        java.util.concurrent.atomic.AtomicLong counter = new java.util.concurrent.atomic.AtomicLong(1000);
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<DataSources.OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((e, ts) -> counter.getAndAdd(1000)));

        // Stream 2: Shipments (simulated)
        DataStream<Shipment> shipments = env.fromData(
            new Shipment("ord-1", "DELIVERED", 1000L),
            new Shipment("ord-2", "SHIPPED", 2000L),
            new Shipment("ord-3", "DELIVERED", 3000L),
            new Shipment("ord-4", "SHIPPED", 4000L)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Shipment>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, ts) -> e.timestamp));

        // Interval join: match orders with shipments where shipment is within 5s after order
        CollectingSink<String> sink = new CollectingSink<>();
        orders.keyBy(DataSources.OrderEvent::orderId)
            .intervalJoin(shipments.keyBy(Shipment::orderId))
            .between(Duration.ofSeconds(-2), Duration.ofSeconds(5))
            .process(new ProcessJoinFunction<DataSources.OrderEvent, Shipment, String>() {
                @Override
                public void processElement(DataSources.OrderEvent order, Shipment shipment,
                                           Context ctx, Collector<String> out) {
                    out.collect(String.format("MATCHED: %s -> %s", order.orderId(), shipment.status()));
                }
            }).sinkTo(sink);

        env.execute("Exercise 16 — Stream Joins");
        return sink.getValues();
    }
}
