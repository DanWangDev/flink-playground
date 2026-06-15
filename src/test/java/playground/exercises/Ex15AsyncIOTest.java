package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 15 — Async I/O")
class Ex15AsyncIOTest extends MiniClusterTestBase {
    @Test
    void shouldEnrichOrdersAsync() throws Exception {
        var ex = new Ex15AsyncIO();
        List<?> results = ex.run(env);
        assertThat(results).hasSize(8);
        assertThat(results).allMatch(r -> r.toString().contains("delay="));
    }
}
