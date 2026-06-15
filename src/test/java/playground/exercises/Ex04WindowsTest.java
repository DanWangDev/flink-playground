package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 04 — Windows")
class Ex04WindowsTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should produce tumbling window results")
    void shouldProduceTumblingWindows() throws Exception {
        var ex = new Ex04Windows();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        assertThat(rows).isNotEmpty();
        assertThat(rows).anyMatch(r -> r.startsWith("tumbling "));
    }

    @Test
    @DisplayName("Should produce sliding window results")
    void shouldProduceSlidingWindows() throws Exception {
        var ex = new Ex04Windows();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        assertThat(rows).anyMatch(r -> r.startsWith("sliding "));
    }

    @Test
    @DisplayName("Should produce session window results")
    void shouldProduceSessionWindows() throws Exception {
        var ex = new Ex04Windows();
        List<?> results = ex.run(env);

        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) results;

        assertThat(rows).anyMatch(r -> r.startsWith("session "));
    }
}
