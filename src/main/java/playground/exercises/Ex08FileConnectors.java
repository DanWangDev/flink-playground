package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import playground.shared.CollectingSink;
import playground.shared.ExerciseRunner;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class Ex08FileConnectors extends ExerciseRunner {

    public Ex08FileConnectors() {
        super("08-file-connectors", "File Connectors");
    }

    @Override
    public List<?> run(StreamExecutionEnvironment env) throws Exception {
        // Create temp input file
        File tmpFile = File.createTempFile("flink-input-", ".txt");
        tmpFile.deleteOnExit();
        try (FileWriter w = new FileWriter(tmpFile)) {
            w.write("record-alpha\nrecord-beta\nrecord-gamma\n");
        }

        // File source
        log.section("Step 1: Reading from Files");
        FileSource<String> source = FileSource.forRecordStreamFormat(
            new TextLineInputFormat(), new Path(tmpFile.toURI())).build();
        DataStream<String> lines = env.fromSource(source,
            WatermarkStrategy.noWatermarks(), "file-source");

        // File sink
        log.section("Step 2: Writing to Files");
        Path outputPath = new Path(System.getProperty("java.io.tmpdir") + "/flink-output-" + System.currentTimeMillis());
        FileSink<String> sink = FileSink.forRowFormat(
            outputPath, new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
            .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("out").build())
            .withRollingPolicy(OnCheckpointRollingPolicy.build())
            .build();

        CollectingSink<String> collectingSink = new CollectingSink<>();
        lines.map(line -> "TRANSFORMED:" + line).returns(String.class)
            .sinkTo(collectingSink);

        env.execute("Exercise 08 — File Connectors");
        return collectingSink.getValues();
    }
}
