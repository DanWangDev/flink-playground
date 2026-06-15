package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.ArrayList;
import java.util.List;

public class Ex18WindowTVFs extends ExerciseRunner {

    public Ex18WindowTVFs() {
        super("18-window-tvfs", "Window TVFs in SQL");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        tEnv.createTemporaryView("orders", orders,
            Schema.newBuilder()
                .column("orderId", DataTypes.STRING())
                .column("customerId", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .column("ts", DataTypes.BIGINT())
                .watermark("ts", "ts - INTERVAL '5' SECOND")
                .build());

        // TUMBLE: fixed-size, non-overlapping windows
        Table tumbleResult = tEnv.sqlQuery(
            "SELECT window_start, window_end, customerId, " +
            "SUM(amount) AS total FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '10' SECONDS)) " +
            "GROUP BY window_start, window_end, customerId");
        DataStream<Row> tumbleStream = tEnv.toChangelogStream(tumbleResult);
        CollectingSink<Row> sink = new CollectingSink<>();
        tumbleStream.sinkTo(sink);

        env.execute("Exercise 18 — Window TVFs");
        List<String> output = new ArrayList<>();
        sink.getValues().forEach(r -> output.add(r.toString()));
        return output;
    }
}
