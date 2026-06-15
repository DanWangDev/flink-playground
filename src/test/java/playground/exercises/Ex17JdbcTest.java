package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 17 — JDBC Connector")
class Ex17JdbcTest extends MiniClusterTestBase {
    @Test
    void shouldInteractWithDatabase() throws Exception {
        var ex = new Ex17Jdbc();
        List<?> results = ex.run(env);
        assertThat(results).hasSize(8);
        assertThat(results).allMatch(r -> r.toString().contains("INSERT INTO"));
    }
}
