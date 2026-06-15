package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.time.Duration;
import java.util.List;

public class Ex24ProcessingTime extends ExerciseRunner {

    public Ex24ProcessingTime() {
        super("24-processing-time", "Processing Time Semantics");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        // Attach event-time timestamps
        DataStream<DataSources.OrderEvent> withTimestamps = orders.assignTimestampsAndWatermarks(
            WatermarkStrategy.<DataSources.OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, ts) -> System.currentTimeMillis()));

        CollectingSink<String> sink = new CollectingSink<>();
        withTimestamps.process(new ProcessFunction<DataSources.OrderEvent, String>() {
            @Override
            public void processElement(DataSources.OrderEvent order, Context ctx,
                                       Collector<String> out) {
                long now = ctx.timerService().currentProcessingTime();
                long watermark = ctx.timerService().currentWatermark();
                out.collect(String.format("order=%s amount=%.0f processingTime=%d watermark=%d",
                    order.orderId(), order.amount(), now, watermark));
            }
        }).sinkTo(sink);

        env.execute("Exercise 24 — Processing Time");
        return sink.getValues();
    }
}
