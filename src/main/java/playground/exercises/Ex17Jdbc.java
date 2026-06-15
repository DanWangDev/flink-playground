package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;

/**
 * Exercise 17 — JDBC Connector
 * Uses H2 in-memory database for zero-install local development.
 * Flink 2.0 moved JDBC to Table/SQL API; this exercise demonstrates
 * direct JDBC integration within a map function.
 */
public class Ex17Jdbc extends ExerciseRunner {

    private static final String DB_URL = "jdbc:h2:mem:playground;DB_CLOSE_DELAY=-1";

    public Ex17Jdbc() {
        super("17-jdbc", "JDBC Connector");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Setup H2 catalog
        try (Connection c = DriverManager.getConnection(DB_URL);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE products (product_id VARCHAR PRIMARY KEY, price DECIMAL(10,2))");
            s.execute("INSERT INTO products VALUES ('P1', 9.99), ('P2', 19.99), ('P3', 29.99)");
        }

        // Enrich orders with product prices from DB
        DataStream<DataSources.OrderEvent> orders = DataSources.orders(env);
        CollectingSink<String> sink = new CollectingSink<>();

        orders.map(o -> {
            String pid = "P" + (Math.abs(o.orderId().hashCode()) % 3 + 1);
            try (Connection c = DriverManager.getConnection(DB_URL);
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT price FROM products WHERE product_id='" + pid + "'")) {
                rs.next();
                return String.format("order=%s product=%s dbPrice=%.2f amount=%.2f",
                    o.orderId(), pid, rs.getDouble("price"), o.amount());
            } catch (Exception e) {
                return String.format("order=%s product=%s dbPrice=ERR amount=%.2f",
                    o.orderId(), pid, o.amount());
            }
        }).returns(String.class).sinkTo(sink);

        env.execute("Exercise 17 — JDBC");
        return sink.getValues();
    }
}
