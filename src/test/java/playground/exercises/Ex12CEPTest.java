package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 12 — CEP")
class Ex12CEPTest extends MiniClusterTestBase {
    @Test
    void shouldDetectDoubleLogin() throws Exception {
        var ex = new Ex12CEP();
        List<?> results = ex.run(env);
        assertThat(results).isNotEmpty();
        assertThat(results).anyMatch(r -> r.toString().contains("SUSPICIOUS"));
    }
}
