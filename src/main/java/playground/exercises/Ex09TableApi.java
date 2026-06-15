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

import static org.apache.flink.table.api.Expressions.$;

public class Ex09TableApi extends ExerciseRunner {

    public Ex09TableApi() {
        super("09-table-api", "Table API");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Register orders as a table
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        Table ordersTable = tableEnv.fromDataStream(orders,
            Schema.newBuilder()
                .column("orderId", DataTypes.STRING())
                .column("customerId", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .build());

        tableEnv.createTemporaryView("orders", ordersTable);

        // SELECT with filter and aggregation
        Table result = tableEnv.sqlQuery(
            "SELECT customerId, CAST(SUM(amount) AS DECIMAL(10,2)) AS total FROM orders GROUP BY customerId");

        DataStream<Row> resultStream = tableEnv.toChangelogStream(result);
        List<String> output = new ArrayList<>();
        CollectingSink<Row> sink = new CollectingSink<>();
        resultStream.sinkTo(sink);

        env.execute("Exercise 09 — Table API");
        sink.getValues().forEach(row -> output.add(row.toString()));
        output.forEach(r -> System.out.println("  " + r));
        return output;
    }
}
