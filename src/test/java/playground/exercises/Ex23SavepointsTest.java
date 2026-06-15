package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 23 — Savepoints")
class Ex23SavepointsTest extends MiniClusterTestBase {
    @Test
    void shouldTrackRunningTotals() throws Exception {
        var ex = new Ex23Savepoints();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().contains("runningTotal="));
    }
}
