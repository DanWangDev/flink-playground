package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 10 — Flink SQL")
class Ex10FlinkSqlTest extends MiniClusterTestBase {
    @Test
    void shouldExecuteSqlWithTumbleWindow() throws Exception {
        var ex = new Ex10FlinkSql();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
    }
}
