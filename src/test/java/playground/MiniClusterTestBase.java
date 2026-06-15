package playground;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import playground.shared.CollectingSink;

/**
 * Base class for MiniCluster integration tests.
 * Each test gets a fresh, isolated local Flink environment.
 *
 * <p>Usage:
 * <pre>{@code
 *   class MyExerciseTest extends MiniClusterTestBase {
 *       @Test
 *       void shouldProduceExpectedResults() throws Exception {
 *           var ex = new Ex01FirstJob();
 *           var results = ex.run(env);
 *           assertThat(results).hasSize(5);
 *       }
 *   }
 * }</pre>
 */
public abstract class MiniClusterTestBase {

    protected StreamExecutionEnvironment env;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setParallelism(1);
        CollectingSink.clearAll();
    }

    @AfterEach
    void tearDown() {
        CollectingSink.clearAll();
    }
}
