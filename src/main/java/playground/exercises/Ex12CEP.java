package playground.exercises;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.List;
import java.util.Map;

public class Ex12CEP extends ExerciseRunner {

    public Ex12CEP() {
        super("12-cep", "CEP — Complex Event Processing");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.LoginEvent> events = DataSources.loginEvents(env);

        // Pattern: LOGIN followed by LOGIN (suspicious double login)
        Pattern<DataSources.LoginEvent, ?> suspiciousLogin = Pattern
            .<DataSources.LoginEvent>begin("first")
            .where(new SimpleCondition<DataSources.LoginEvent>() {
                @Override
                public boolean filter(DataSources.LoginEvent e) {
                    return "LOGIN".equals(e.action());
                }
            })
            .next("second")
            .where(new SimpleCondition<DataSources.LoginEvent>() {
                @Override
                public boolean filter(DataSources.LoginEvent e) {
                    return "LOGIN".equals(e.action());
                }
            });

        PatternStream<DataSources.LoginEvent> patternStream = CEP.pattern(
            events.keyBy(DataSources.LoginEvent::userId), suspiciousLogin);

        CollectingSink<String> sink = new CollectingSink<>();
        patternStream.process(new PatternProcessFunction<DataSources.LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<DataSources.LoginEvent>> match,
                                     Context ctx, Collector<String> out) {
                DataSources.LoginEvent first = match.get("first").get(0);
                DataSources.LoginEvent second = match.get("second").get(0);
                out.collect(String.format("SUSPICIOUS: user=%s double login at %d and %d",
                    first.userId(), first.timestamp(), second.timestamp()));
            }
        }).sinkTo(sink);

        env.execute("Exercise 12 — CEP");
        return sink.getValues();
    }
}
