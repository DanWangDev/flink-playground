package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 02 — Keyed Streams")
class Ex02KeyedStreamsTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should produce reduce results per customer")
    void shouldProduceReduceResults() throws Exception {
        var exercise = new Ex02KeyedStreams();
        List<?> results = exercise.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        // Should have results from all 3 sub-jobs
        assertThat(rows).isNotEmpty();

        // Reduce results should show per-customer totals
        assertThat(rows)
            .anyMatch(r -> r.contains("total=") && r.contains("customer=cust-a"))
            .anyMatch(r -> r.contains("total=") && r.contains("customer=cust-b"))
            .anyMatch(r -> r.contains("total=") && r.contains("customer=cust-c"));
    }

    @Test
    @DisplayName("Should count orders per customer with RichMapFunction")
    void shouldCountPerKey() throws Exception {
        var exercise = new Ex02KeyedStreams();
        List<?> results = exercise.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        // Per-key counter should show incrementing counts per customer
        assertThat(rows)
            .anyMatch(r -> r.contains("orderCount=1"))
            .anyMatch(r -> r.contains("orderCount=3"))
            .anyMatch(r -> r.contains("orderCount=2"));
    }

    @Test
    @DisplayName("Should produce tuple aggregation results")
    void shouldProduceTupleResults() throws Exception {
        var exercise = new Ex02KeyedStreams();
        List<?> results = exercise.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        // Tuple results should show totalSpend per customer
        assertThat(rows)
            .anyMatch(r -> r.contains("totalSpend="));
    }
}
