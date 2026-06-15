package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import playground.shared.CollectingSink;
import playground.shared.ExerciseRunner;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class Ex20FileFormats extends ExerciseRunner {

    public Ex20FileFormats() {
        super("20-file-formats", "File Sink & Formats");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Try file source, fall back to in-memory if permissions don't allow writing
        DataStream<String> lines;
        try {
            File dataDir = new File("data");
            if (!dataDir.exists()) dataDir.mkdirs();
            File inputFile = new File(dataDir, "ex20-input.csv");
            try (FileWriter w = new FileWriter(inputFile)) {
                w.write("ord-A,cust-1,99.99\nord-B,cust-2,149.50\nord-C,cust-1,49.99\n");
            }
            FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path(inputFile.toURI())).build();
            lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");
            log.info("Using FileSource from " + inputFile.getAbsolutePath());
        } catch (Exception e) {
            log.warn("File access failed, using in-memory source");
            lines = env.fromData(
                "ord-A,cust-1,99.99", "ord-B,cust-2,149.50", "ord-C,cust-1,49.99"
            ).name("fallback-source");
        }

        CollectingSink<String> collectingSink = new CollectingSink<>();
        lines.map(line -> {
            String[] parts = line.split(",");
            return String.format("{\"orderId\":\"%s\",\"customer\":\"%s\",\"amount\":%s}",
                parts[0], parts[1], parts[2]);
        }).returns(String.class).sinkTo(collectingSink);

        env.execute("Exercise 20 — File Formats");
        return collectingSink.getValues();
    }
}
