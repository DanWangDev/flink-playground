package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 06 — Checkpointing")
class Ex06CheckpointingTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should process orders with checkpointing enabled")
    void shouldRunWithCheckpointing() throws Exception {
        var ex = new Ex06Checkpointing();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        assertThat(rows).isNotEmpty();
        assertThat(rows).anyMatch(r -> r.contains("runningTotal="));
        assertThat(rows).anyMatch(r -> r.contains("customer=cust-a"));
    }

    @Test
    @DisplayName("Should accumulate running totals per customer")
    void shouldAccumulateTotals() throws Exception {
        var ex = new Ex06Checkpointing();
        List<?> results = ex.run(env);
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;
        // Cust-a has 3 orders (100+75+200=375 total)
        assertThat(rows).anyMatch(r -> r.contains("runningTotal=375.00"));
        // Cust-b has 3 orders (250+150+175=575 total)
        assertThat(rows).anyMatch(r -> r.contains("runningTotal=575.00"));
    }
}
