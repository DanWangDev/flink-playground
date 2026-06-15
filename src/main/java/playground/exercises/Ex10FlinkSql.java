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

public class Ex10FlinkSql extends ExerciseRunner {

    public Ex10FlinkSql() {
        super("10-flink-sql", "Flink SQL");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Register DataStream as table with a simple schema
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        tableEnv.createTemporaryView("orders", orders,
            Schema.newBuilder()
                .column("orderId", DataTypes.STRING())
                .column("customerId", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .build());

        // SQL aggregation
        Table result = tableEnv.sqlQuery(
            "SELECT customerId, COUNT(*) AS cnt, CAST(SUM(amount) AS DECIMAL(10,2)) AS total " +
            "FROM orders GROUP BY customerId");

        DataStream<Row> resultStream = tableEnv.toChangelogStream(result);
        CollectingSink<Row> sink = new CollectingSink<>();
        resultStream.sinkTo(sink);

        env.execute("Exercise 10 — Flink SQL");
        List<String> output = new ArrayList<>();
        sink.getValues().forEach(row -> output.add(row.toString()));
        return output;
    }
}
