package playground.exercises;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import playground.shared.CollectingSink;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 01 — Your First Flink Job
 * ==================================
 * Learn the fundamentals: StreamExecutionEnvironment, DataStream creation,
 * and basic transformations (map, filter, flatMap).
 *
 * <p>Concepts introduced:
 * - StreamExecutionEnvironment (the entry point for all Flink jobs)
 * - DataStream (a potentially unbounded stream of records)
 * - map, filter, flatMap (stateless transformations)
 * - CollectingSink (captures results for test assertions)
 */
public class Ex01FirstJob extends ExerciseRunner {

    public Ex01FirstJob() {
        super("01-first-job", "Your First Flink Job");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Step 1: Create a DataStream ──
        log.section("Step 1: Creating Your First DataStream");
        log.concept(
            "A DataStream is the core abstraction in Flink. It represents a potentially " +
            "unbounded stream of records. You create one from a source — here we use " +
            "env.fromData() to create a bounded stream from an in-memory collection."
        );

        DataStream<String> sentences = env.fromData(
            "Apache Flink is a powerful stream processing framework",
            "It supports event time and processing time semantics",
            "Flink provides exactly-once state consistency",
            "The DataStream API is the foundation of Flink",
            "Stream processing enables real-time data analytics"
        ).name("sentence-source");

        step.pause("DataStream created from 5 sentences");

        // ── Step 2: Transform with flatMap ──
        log.section("Step 2: Splitting Sentences into Words with flatMap");
        log.concept(
            "flatMap takes one element and produces zero, one, or more elements. " +
            "Here we split each sentence into individual words. This is a 1-to-many " +
            "transformation — each sentence produces multiple words."
        );

        DataStream<String> words = sentences
            .flatMap((String sentence, org.apache.flink.util.Collector<String> out) -> {
                for (String word : sentence.toLowerCase().replaceAll("[^a-z ]", "").split("\\s+")) {
                    if (!word.isEmpty()) {
                        out.collect(word);
                    }
                }
            })
            .returns(String.class)
            .name("word-splitter");

        step.pause("Sentences split into words with flatMap");

        // ── Step 3: Transform with filter ──
        log.section("Step 3: Filtering Short Words");
        log.concept(
            "filter keeps only elements that satisfy a predicate. Here we remove " +
            "short words (length <= 2) like 'is', 'a', 'it', 'of'. This reduces " +
            "noise and focuses on meaningful content words."
        );

        DataStream<String> filtered = words
            .filter(word -> word.length() > 2)
            .name("word-filter");

        step.pause("Short words removed with filter");

        // ── Step 4: Transform with map ──
        log.section("Step 4: Capitalizing Words with map");
        log.concept(
            "map is a 1-to-1 transformation: each input element produces exactly one " +
            "output element. Here we capitalize the first letter of each word. " +
            "Unlike flatMap, map cannot change the number of elements."
        );

        DataStream<String> capitalized = filtered
            .map(word -> word.substring(0, 1).toUpperCase() + word.substring(1))
            .returns(String.class)
            .name("word-capitalizer");

        step.pause("Words capitalized with map");

        // ── Step 5: Collect and verify results ──
        log.section("Step 5: Collecting Results");
        log.concept(
            "CollectingSink is a special sink that captures stream output into a List. " +
            "In production you'd use a real sink (Kafka, files, database), but for " +
            "exercises and tests, collecting into memory gives us deterministic results " +
            "we can assert on."
        );

        CollectingSink<String> sink = new CollectingSink<>();
        capitalized.sinkTo(sink).name("collecting-sink");

        log.info("Executing Flink job...");
        env.execute("Exercise 01 — First Job");

        List<String> results = sink.getValues();
        log.success("Collected " + results.size() + " words:");
        results.forEach(word -> log.output("  • " + word));

        return results;
    }
}
