package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 14 — Production Patterns")
class Ex14ProductionPatternsTest extends MiniClusterTestBase {
    @Test
    void shouldTrackMetrics() throws Exception {
        var ex = new Ex14ProductionPatterns();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().contains("metrics="));
    }
}
