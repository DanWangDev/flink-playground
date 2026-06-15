package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 22 — Restart Strategies")
class Ex22RestartStrategiesTest extends MiniClusterTestBase {
    @Test
    void shouldTrackCountsAcrossRestart() throws Exception {
        var ex = new Ex22RestartStrategies();
        List<?> results = ex.run(env);
        assertThat(results).hasSize(8);
        assertThat(results).allMatch(r -> r.toString().contains("#"));
    }
}
