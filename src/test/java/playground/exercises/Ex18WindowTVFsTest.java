package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 18 — Window TVFs")
class Ex18WindowTVFsTest extends MiniClusterTestBase {
    @Test
    void shouldProduceTumbleAggregates() throws Exception {
        var ex = new Ex18WindowTVFs();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
    }
}
