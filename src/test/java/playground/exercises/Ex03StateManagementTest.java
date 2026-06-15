package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 03 — State Management")
class Ex03StateManagementTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should count clicks per user with ValueState")
    void shouldCountClicksPerUser() throws Exception {
        var ex = new Ex03StateManagement();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        assertThat(rows).isNotEmpty();
        // user-1 has 3 clicks, should see incrementing counts
        assertThat(rows).anyMatch(r -> r.contains("clicks=1"));
        assertThat(rows).anyMatch(r -> r.contains("clicks=2"));
        assertThat(rows).anyMatch(r -> r.contains("clicks=3"));
    }

    @Test
    @DisplayName("Should collect pages per user with ListState")
    void shouldCollectPagesPerUser() throws Exception {
        var ex = new Ex03StateManagement();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        // ListState accumulates visited pages
        assertThat(rows).anyMatch(r -> r.contains("pages=") && r.contains("→"));
    }

    @Test
    @DisplayName("Should count per-page with MapState")
    void shouldCountPerPage() throws Exception {
        var ex = new Ex03StateManagement();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        // MapState shows per-page counts per user
        assertThat(rows).anyMatch(r -> r.contains("page-a="));
    }
}
