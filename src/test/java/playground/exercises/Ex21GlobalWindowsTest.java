package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 21 — Global Windows")
class Ex21GlobalWindowsTest extends MiniClusterTestBase {
    @Test
    void shouldFireOnCountTrigger() throws Exception {
        var ex = new Ex21GlobalWindows();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().contains("batch-total="));
    }
}
