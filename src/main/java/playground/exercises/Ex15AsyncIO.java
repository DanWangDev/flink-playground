package playground.exercises;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Ex15AsyncIO extends ExerciseRunner {

    public Ex15AsyncIO() {
        super("15-async-io", "Async I/O");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);

        // Simulate exchange rate lookup service
        AsyncFunction<DataSources.OrderEvent, String> rateLookup =
            new AsyncFunction<DataSources.OrderEvent, String>() {
                @Override
                public void asyncInvoke(DataSources.OrderEvent order,
                                        ResultFuture<String> resultFuture) {
                    CompletableFuture.supplyAsync(() -> {
                        // Simulate 50-200ms latency
                        long delay = ThreadLocalRandom.current().nextLong(50, 200);
                        try { Thread.sleep(delay); } catch (InterruptedException ignored) {}
                        double rate = 1.0 + ThreadLocalRandom.current().nextDouble() * 0.1;
                        return String.format("%s: $%.2f x %.4f = $%.2f (delay=%dms)",
                            order.orderId(), order.amount(), rate,
                            order.amount() * rate, delay);
                    }).thenAcceptAsync((String result) ->
                        resultFuture.complete(Collections.singleton(result)));
                }
            };

        CollectingSink<String> sink = new CollectingSink<>();
        AsyncDataStream.unorderedWait(orders, rateLookup, 5000, TimeUnit.MILLISECONDS, 4)
            .returns(String.class)
            .name("async-enrichment")
            .sinkTo(sink);

        env.execute("Exercise 15 — Async I/O");
        return sink.getValues();
    }
}
