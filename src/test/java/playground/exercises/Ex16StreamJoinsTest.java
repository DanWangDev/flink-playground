package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 16 — Stream Joins")
class Ex16StreamJoinsTest extends MiniClusterTestBase {
    @Test
    void shouldMatchOrdersWithShipments() throws Exception {
        var ex = new Ex16StreamJoins();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().startsWith("MATCHED:"));
    }
}
