package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.ArrayList;
import java.util.List;

public class Ex19UDF extends ExerciseRunner {

    // Custom ScalarFunction: calculate tax
    public static class TaxCalculator extends ScalarFunction {
        public double eval(double amount) { return Math.round(amount * 0.08 * 100.0) / 100.0; }
    }

    public Ex19UDF() {
        super("19-udf", "User-Defined Functions (UDF)");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Register UDF
        tEnv.createTemporarySystemFunction("TAX", TaxCalculator.class);

        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        tEnv.createTemporaryView("orders", orders,
            Schema.newBuilder()
                .column("orderId", DataTypes.STRING())
                .column("customerId", DataTypes.STRING())
                .column("amount", DataTypes.DOUBLE())
                .build());

        // SQL query using the UDF
        Table result = tEnv.sqlQuery(
            "SELECT orderId, amount, TAX(amount) AS tax " +
            "FROM orders WHERE amount > 100");

        DataStream<Row> resultStream = tEnv.toChangelogStream(result);
        CollectingSink<Row> sink = new CollectingSink<>();
        resultStream.sinkTo(sink);

        env.execute("Exercise 19 — UDF");
        List<String> output = new ArrayList<>();
        sink.getValues().forEach(r -> output.add(r.toString()));
        return output;
    }
}
