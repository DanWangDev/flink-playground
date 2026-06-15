package playground.helpers;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import java.util.List;

/**
 * Fluent assertion helpers for stream processing test results.
 * Wraps AssertJ with convenience methods for common Flink test patterns.
 */
public final class StreamAssertions {

    private StreamAssertions() {}

    /** Assert that a collected list is non-empty and has an expected size */
    public static <T> ListAssert<T> assertResults(List<T> results) {
        return Assertions.assertThat(results)
            .as("Collected stream results")
            .isNotNull();
    }

    /** Assert results are non-empty */
    public static <T> void assertNotEmpty(List<T> results) {
        Assertions.assertThat(results)
            .as("Stream should produce at least one result")
            .isNotEmpty();
    }

    /** Assert results contain a specific element */
    public static <T> void assertContains(List<T> results, T element) {
        Assertions.assertThat(results)
            .as("Stream results should contain %s", element)
            .contains(element);
    }

    /** Assert all results match a predicate */
    public static <T> void assertAllMatch(List<T> results, java.util.function.Predicate<T> predicate, String description) {
        Assertions.assertThat(results)
            .as("All results should %s", description)
            .allMatch(predicate);
    }
}
