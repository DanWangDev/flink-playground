package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 13 — Broadcast State")
class Ex13BroadcastStateTest extends MiniClusterTestBase {
    @Test
    void shouldApplyDiscounts() throws Exception {
        var ex = new Ex13BroadcastState();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        // cust-a gets 10% off, cust-b gets 5% off
        assertThat(results).anyMatch(r -> r.toString().contains("10% off"));
        assertThat(results).anyMatch(r -> r.toString().contains("5% off"));
    }
}
