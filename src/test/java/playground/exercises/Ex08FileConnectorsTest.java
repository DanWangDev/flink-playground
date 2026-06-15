package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 08 — File Connectors")
class Ex08FileConnectorsTest extends MiniClusterTestBase {
    @Test
    void shouldReadAndTransformFileRecords() throws Exception {
        var ex = new Ex08FileConnectors();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        assertThat(rows).hasSize(3);
        assertThat(rows).allMatch(r -> r.startsWith("TRANSFORMED:"));
    }
}
