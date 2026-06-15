package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 07 — Kafka Connector")
class Ex07KafkaConnectorTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should process records via in-memory fallback")
    void shouldProcessRecords() throws Exception {
        var ex = new Ex07KafkaConnector();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        assertThat(rows).isNotEmpty();
        assertThat(rows).allMatch(r -> r.startsWith("PROCESSED:"));
    }
}
