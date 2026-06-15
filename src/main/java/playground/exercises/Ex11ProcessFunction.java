package playground.exercises;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import playground.shared.CollectingSink;
import playground.shared.DataSources;
import playground.shared.ExerciseRunner;

import java.util.ArrayList;
import java.util.List;

public class Ex11ProcessFunction extends ExerciseRunner {

    private static final OutputTag<String> ALERTS = new OutputTag<String>("alerts") {};

    public Ex11ProcessFunction() {
        super("11-process-function", "ProcessFunction");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        DataStream<DataSources.TradeEvent> trades = DataSources.trades(env);
        CollectingSink<String> mainSink = new CollectingSink<>();
        CollectingSink<String> alertSink = new CollectingSink<>();

        var result = trades
            .keyBy(DataSources.TradeEvent::symbol)
            .process(new KeyedProcessFunction<String, DataSources.TradeEvent, String>() {
                private ValueState<Double> lastPrice;

                @Override
                public void open(OpenContext ctx) {
                    lastPrice = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("lastPrice", Double.class));
                }

                @Override
                public void processElement(DataSources.TradeEvent trade, Context ctx,
                                           Collector<String> out) throws Exception {
                    Double prev = lastPrice.value();
                    if (prev != null) {
                        double change = ((trade.price() - prev) / prev) * 100;
                        out.collect(String.format("%s: $%.2f (%.1f%%)", trade.symbol(), trade.price(), change));
                        if (Math.abs(change) > 1.0) {
                            ctx.output(ALERTS, String.format("ALERT: %s moved %.1f%%", trade.symbol(), change));
                        }
                    } else {
                        out.collect(String.format("%s: $%.2f (first trade)", trade.symbol(), trade.price()));
                    }
                    lastPrice.update(trade.price());
                }
            });

        result.sinkTo(mainSink);
        result.getSideOutput(ALERTS).sinkTo(alertSink);

        env.execute("Exercise 11 — ProcessFunction");
        List<String> all = new ArrayList<>();
        all.addAll(mainSink.getValues());
        all.addAll(alertSink.getValues());
        return all;
    }
}
