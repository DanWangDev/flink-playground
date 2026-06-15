package playground.shared;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Static factory methods for creating bounded test data streams.
 */
public final class DataSources {

    private DataSources() {}

    // Words stream
    public static DataStream<String> words(StreamExecutionEnvironment env) {
        return env.fromData(
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"
        ).returns(Types.STRING);
    }

    // Numbers stream
    public static DataStream<Integer> numbers(StreamExecutionEnvironment env) {
        return env.fromData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            .returns(Types.INT);
    }

    // Order events
    public static DataStream<OrderEvent> orders(StreamExecutionEnvironment env) {
        List<OrderEvent> data = Arrays.asList(
            new OrderEvent("ord-1", "cust-a", 100.0),
            new OrderEvent("ord-2", "cust-b", 250.0),
            new OrderEvent("ord-3", "cust-a", 75.0),
            new OrderEvent("ord-4", "cust-c", 300.0),
            new OrderEvent("ord-5", "cust-b", 150.0),
            new OrderEvent("ord-6", "cust-a", 200.0),
            new OrderEvent("ord-7", "cust-c", 50.0),
            new OrderEvent("ord-8", "cust-b", 175.0)
        );
        return env.fromData(data).returns(OrderEvent.class);
    }

    // Sensor readings with timestamps
    public static DataStream<SensorReading> sensors(StreamExecutionEnvironment env) {
        List<SensorReading> data = Arrays.asList(
            new SensorReading("sensor-1", 22.5, 1000L),
            new SensorReading("sensor-1", 23.1, 2000L),
            new SensorReading("sensor-2", 18.3, 1500L),
            new SensorReading("sensor-1", 22.8, 4000L),
            new SensorReading("sensor-2", 19.0, 3500L),
            new SensorReading("sensor-1", 24.0, 5000L),
            new SensorReading("sensor-2", 20.5, 6000L),
            new SensorReading("sensor-1", 25.2, 7000L)
        );
        return env.fromData(data)
            .returns(SensorReading.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> event.timestamp)
            );
    }

    // Click events
    public static DataStream<ClickEvent> clicks(StreamExecutionEnvironment env) {
        List<ClickEvent> data = Arrays.asList(
            new ClickEvent("user-1", "page-a"),
            new ClickEvent("user-2", "page-b"),
            new ClickEvent("user-1", "page-c"),
            new ClickEvent("user-3", "page-a"),
            new ClickEvent("user-2", "page-a"),
            new ClickEvent("user-1", "page-b"),
            new ClickEvent("user-3", "page-c"),
            new ClickEvent("user-2", "page-c")
        );
        return env.fromData(data).returns(ClickEvent.class);
    }

    // Trade events
    public static DataStream<TradeEvent> trades(StreamExecutionEnvironment env) {
        List<TradeEvent> data = Arrays.asList(
            new TradeEvent("AAPL", 150.0, 100, 1000L),
            new TradeEvent("AAPL", 151.0, 50, 2000L),
            new TradeEvent("GOOG", 2800.0, 10, 1500L),
            new TradeEvent("AAPL", 152.0, 200, 4000L),
            new TradeEvent("GOOG", 2795.0, 25, 5000L),
            new TradeEvent("MSFT", 420.0, 75, 3000L),
            new TradeEvent("AAPL", 149.0, 150, 6000L),
            new TradeEvent("MSFT", 422.0, 50, 7000L)
        );
        return env.fromData(data)
            .returns(TradeEvent.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, timestamp) -> event.timestamp)
            );
    }

    // Login events for CEP
    public static DataStream<LoginEvent> loginEvents(StreamExecutionEnvironment env) {
        List<LoginEvent> data = Arrays.asList(
            new LoginEvent("user-1", "LOGIN", 1000L),
            new LoginEvent("user-1", "LOGOUT", 2000L),
            new LoginEvent("user-2", "LOGIN", 1500L),
            new LoginEvent("user-1", "LOGIN", 3000L),
            new LoginEvent("user-1", "LOGIN", 3100L),
            new LoginEvent("user-2", "LOGOUT", 4000L),
            new LoginEvent("user-1", "LOGOUT", 5000L),
            new LoginEvent("user-2", "LOGIN", 6000L)
        );
        return env.fromData(data)
            .returns(LoginEvent.class)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, timestamp) -> event.timestamp)
            );
    }

    // Record types

    public record OrderEvent(String orderId, String customerId, double amount) {
        @Override
        public String toString() {
            return String.format("Order{id=%s, customer=%s, amount=%.2f}", orderId, customerId, amount);
        }
    }

    public record SensorReading(String sensorId, double temperature, long timestamp) {
        @Override
        public String toString() {
            return String.format("Sensor{id=%s, temp=%.1f, ts=%d}", sensorId, temperature, timestamp);
        }
    }

    public record ClickEvent(String userId, String page) {
        @Override
        public String toString() {
            return String.format("Click{user=%s, page=%s}", userId, page);
        }
    }

    public record TradeEvent(String symbol, double price, int volume, long timestamp) {
        @Override
        public String toString() {
            return String.format("Trade{symbol=%s, price=%.2f, volume=%d, ts=%d}", symbol, price, volume, timestamp);
        }
    }

    public record LoginEvent(String userId, String action, long timestamp) {
        @Override
        public String toString() {
            return String.format("Login{user=%s, action=%s, ts=%d}", userId, action, timestamp);
        }
    }
}
