package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 19 — UDF")
class Ex19UDFTest extends MiniClusterTestBase {
    @Test
    void shouldApplyTaxUDF() throws Exception {
        var ex = new Ex19UDF();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
    }
}
