package playground.exercises;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import playground.MiniClusterTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Exercise 01 — First Job")
class Ex01FirstJobTest extends MiniClusterTestBase {

    @Test
    @DisplayName("Should split sentences into capitalized words longer than 2 chars")
    void shouldProduceCapitalizedWords() throws Exception {
        var exercise = new Ex01FirstJob();
        List<?> results = exercise.run(env);

        @SuppressWarnings("unchecked")
        List<String> words = (List<String>) results;

        assertThat(words)
            .isNotEmpty()
            .contains("Apache", "Flink", "Powerful", "Stream", "Processing", "Framework")
            .doesNotContain("is", "a", "it", "of", "the")
            .allMatch(word -> word.length() > 2);
    }

    @Test
    @DisplayName("Should not contain short words (length <= 2)")
    void shouldFilterShortWords() throws Exception {
        var exercise = new Ex01FirstJob();
        List<?> results = exercise.run(env);

        @SuppressWarnings("unchecked")
        List<String> words = (List<String>) results;

        assertThat(words)
            .allMatch(word -> word.length() > 2)
            .noneMatch(word -> word.equals("is"))
            .noneMatch(word -> word.equals("a"))
            .noneMatch(word -> word.equals("it"))
            .noneMatch(word -> word.equals("of"));
    }

    @Test
    @DisplayName("Should produce at least 10 content words")
    void shouldProduceManyWords() throws Exception {
        var exercise = new Ex01FirstJob();
        List<?> results = exercise.run(env);

        assertThat(results).hasSizeGreaterThanOrEqualTo(10);
    }
}
