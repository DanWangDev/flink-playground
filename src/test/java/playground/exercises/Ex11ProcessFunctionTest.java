package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 11 — ProcessFunction")
class Ex11ProcessFunctionTest extends MiniClusterTestBase {
    @Test
    void shouldDetectPriceChanges() throws Exception {
        var ex = new Ex11ProcessFunction();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results.stream().anyMatch(r -> r.toString().contains("ALERT"))).isTrue();
    }
}
