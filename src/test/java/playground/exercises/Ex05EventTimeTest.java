package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 05 — Event Time & Watermarks")
class Ex05EventTimeTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should produce window results with event time")
    void shouldUseEventTime() throws Exception {
        var ex = new Ex05EventTime();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        assertThat(rows).isNotEmpty();
        assertThat(rows).anyMatch(r -> r.contains("event-time"));
    }

    @Test
    @DisplayName("Should complete without errors and produce results")
    void shouldCompleteWithoutErrors() throws Exception {
        var ex = new Ex05EventTime();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        assertThat(rows).isNotNull();
        assertThat(rows).anyMatch(r -> r.contains("event-time"));
    }
}
