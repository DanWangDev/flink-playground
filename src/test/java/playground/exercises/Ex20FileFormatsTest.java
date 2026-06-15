package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 20 — File Formats")
class Ex20FileFormatsTest extends MiniClusterTestBase {
    @Test
    void shouldProduceJsonOutput() throws Exception {
        var ex = new Ex20FileFormats();
        List<?> results = ex.run(env);
        assertThat(results).hasSize(3);
        assertThat(results).allMatch(r -> r.toString().startsWith("{") && r.toString().contains("\"orderId\""));
    }
}
