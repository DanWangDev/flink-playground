package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;

/**
 * Exercise 03 — State Management
 * ==============================
 * Learn ValueState, ListState, MapState, and state TTL.
 */
public class Ex03StateManagement extends ExerciseRunner {

    public Ex03StateManagement() {
        super("03-state-management", "State Management");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // ── Step 1: ValueState — count per user ──
        log.section("Step 1: ValueState — Counting Clicks Per User");
        log.concept(
            "ValueState holds a single value per key. It's the simplest form of " +
            "managed state. Flink stores it in the state backend (heap or RocksDB) " +
            "and automatically snapshots it during checkpoints."
        );

        DataStream<DataSources.ClickEvent> clicks = DataSources.clicks(env);
        CollectingSink<String> valueStateSink = new CollectingSink<>();

        clicks
            .keyBy(DataSources.ClickEvent::userId)
            .process(new KeyedProcessFunction<String, DataSources.ClickEvent, String>() {
                private ValueState<Integer> clickCount;

                @Override
                public void open(OpenContext ctx) {
                    ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>(
                        "click-count", Integer.class);
                    clickCount = getRuntimeContext().getState(desc);
                }

                @Override
                public void processElement(DataSources.ClickEvent event, Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer current = clickCount.value();
                    int count = (current == null) ? 1 : current + 1;
                    clickCount.update(count);
                    out.collect(String.format("user=%s clicks=%d page=%s",
                        event.userId(), count, event.page()));
                }
            })
            .name("value-state-counter")
            .sinkTo(valueStateSink);

        env.execute("Exercise 03 — ValueState");
        List<String> valueStateResults = valueStateSink.getValues();
        log.success("ValueState results (" + valueStateResults.size() + " records):");
        valueStateResults.forEach(r -> log.output("  " + r));

        // ── Step 2: ListState — collect all pages per user ──
        log.section("Step 2: ListState — Collecting All Pages Per User");
        log.concept(
            "ListState holds a list of values per key. Use it when you need to " +
            "accumulate items over time — like all pages visited by a user, or all " +
            "events in a session."
        );

        var clicks2 = DataSources.clicks(env);
        CollectingSink<String> listStateSink = new CollectingSink<>();

        clicks2
            .keyBy(DataSources.ClickEvent::userId)
            .process(new KeyedProcessFunction<String, DataSources.ClickEvent, String>() {
                private ListState<String> visitedPages;

                @Override
                public void open(OpenContext ctx) {
                    ListStateDescriptor<String> desc = new ListStateDescriptor<>(
                        "visited-pages", String.class);
                    visitedPages = getRuntimeContext().getListState(desc);
                }

                @Override
                public void processElement(DataSources.ClickEvent event, Context ctx,
                                           Collector<String> out) throws Exception {
                    visitedPages.add(event.page());
                    List<String> all = new java.util.ArrayList<>();
                    visitedPages.get().forEach(all::add);
                    out.collect(String.format("user=%s pages=%s",
                        event.userId(), String.join("→", all)));
                }
            })
            .name("list-state-collector")
            .sinkTo(listStateSink);

        env.execute("Exercise 03 — ListState");
        List<String> listStateResults = listStateSink.getValues();
        log.success("ListState results (" + listStateResults.size() + " records):");
        listStateResults.forEach(r -> log.output("  " + r));

        // ── Step 3: MapState — count per page ──
        log.section("Step 3: MapState — Counting Per Page Per User");
        log.concept(
            "MapState is like a HashMap per key — each entry has its own key-value " +
            "pair and can be read/updated independently. More efficient than ListState " +
            "when you need random access to individual entries."
        );

        var clicks3 = DataSources.clicks(env);
        CollectingSink<String> mapStateSink = new CollectingSink<>();

        clicks3
            .keyBy(DataSources.ClickEvent::userId)
            .process(new KeyedProcessFunction<String, DataSources.ClickEvent, String>() {
                private MapState<String, Integer> pageCounts;

                @Override
                public void open(OpenContext ctx) {
                    MapStateDescriptor<String, Integer> desc = new MapStateDescriptor<>(
                        "page-counts", String.class, Integer.class);
                    pageCounts = getRuntimeContext().getMapState(desc);
                }

                @Override
                public void processElement(DataSources.ClickEvent event, Context ctx,
                                           Collector<String> out) throws Exception {
                    Integer current = pageCounts.get(event.page());
                    int count = (current == null) ? 1 : current + 1;
                    pageCounts.put(event.page(), count);

                    StringBuilder sb = new StringBuilder("user=" + event.userId() + " ");
                    pageCounts.iterator().forEachRemaining(
                        e -> sb.append(e.getKey()).append("=").append(e.getValue()).append(" "));
                    out.collect(sb.toString().trim());
                }
            })
            .name("map-state-counter")
            .sinkTo(mapStateSink);

        env.execute("Exercise 03 — MapState");
        List<String> mapStateResults = mapStateSink.getValues();
        log.success("MapState results (" + mapStateResults.size() + " records):");
        mapStateResults.forEach(r -> log.output("  " + r));

        // ── Summary ──
        List<String> allResults = new java.util.ArrayList<>();
        allResults.addAll(valueStateResults);
        allResults.addAll(listStateResults);
        allResults.addAll(mapStateResults);
        return allResults;
    }
}
