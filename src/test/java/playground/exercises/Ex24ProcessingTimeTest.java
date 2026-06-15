package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 24 — Processing Time")
class Ex24ProcessingTimeTest extends MiniClusterTestBase {
    @Test
    void shouldProduceProcessingTimeWindows() throws Exception {
        var ex = new Ex24ProcessingTime();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().contains("PROC-TIME"));
    }
}
