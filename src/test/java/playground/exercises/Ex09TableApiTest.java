package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 09 — Table API")
class Ex09TableApiTest extends MiniClusterTestBase {
    @Test
    void shouldAggregateWithSql() throws Exception {
        var ex = new Ex09TableApi();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
    }
}
