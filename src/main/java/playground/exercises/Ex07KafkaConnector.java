package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import playground.shared.CollectingSink;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 07 — Kafka Connector.
 * Uses in-memory source for MiniCluster tests; real Kafka for Docker integration.
 */
public class Ex07KafkaConnector extends ExerciseRunner {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final boolean KAFKA_AVAILABLE = "true".equals(
        System.getenv("KAFKA_ENABLED"));

    public Ex07KafkaConnector() {
        super("07-kafka-connector", "Kafka Connector");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Kafka Source ──
        log.section("Kafka as a Source");
        log.concept(
            "KafkaSource reads from Kafka topics with exactly-once support. " +
            "In CI/Docker mode, connects to real Kafka. In MiniCluster tests, " +
            "uses an in-memory fallback."
        );

        DataStream<String> input;
        if (KAFKA_AVAILABLE) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("playground-input")
                .setGroupId("ex07-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
            input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
            log.info("Kafka source configured at " + BOOTSTRAP);
        } else {
            input = env.fromData(
                "{\"order\":\"ord-1\",\"amount\":100}",
                "{\"order\":\"ord-2\",\"amount\":250}",
                "{\"order\":\"ord-3\",\"amount\":75}"
            ).name("fallback-source");
            log.info("KAFKA_ENABLED not set — using in-memory source");
        }

        // ── Kafka Sink ──
        log.section("Kafka as a Sink");
        log.concept(
            "KafkaSink writes to Kafka with at-least-once or exactly-once guarantees. " +
            "For testing, results are also collected into memory."
        );

        CollectingSink<String> collectingSink = new CollectingSink<>();
        input
            .map(msg -> "PROCESSED:" + msg)
            .returns(String.class)
            .sinkTo(collectingSink);

        if (KAFKA_AVAILABLE) {
            KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("playground-output")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
                .build();
            input.map(msg -> "PROCESSED:" + msg).returns(String.class).sinkTo(sink);
            log.info("Kafka sink writing to playground-output");
        }

        env.execute("Exercise 07 — Kafka Connector");
        return collectingSink.getValues();
    }
}
