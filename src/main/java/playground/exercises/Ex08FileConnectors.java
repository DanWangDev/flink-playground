package playground.exercises;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        // Try writing to data/ (works locally and in Docker with writable volume)
        // Fall back to in-memory source if file system access fails
        DataStream<String> lines;
        File dataDir = new File("data");
        boolean useFileSource = false;

        try {
            if (!dataDir.exists()) dataDir.mkdirs();
            File inputFile = new File(dataDir, "ex08-input.txt");
            if (inputFile.exists()) inputFile.delete();
            try (FileWriter w = new FileWriter(inputFile)) {
                w.write("record-alpha\nrecord-beta\nrecord-gamma\n");
            }
            FileSource<String> source = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path(inputFile.toURI())).build();
            lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
            useFileSource = true;
            log.info("Using FileSource from " + inputFile.getAbsolutePath());
        } catch (Exception e) {
            log.warn("File access failed, using in-memory source");
            lines = env.fromData("record-alpha", "record-beta", "record-gamma")
                .name("fallback-source");
        }

        CollectingSink<String> collectingSink = new CollectingSink<>();
        lines.map(line -> "TRANSFORMED:" + line).returns(String.class)
            .sinkTo(collectingSink);

        env.execute("Exercise 08 — File Connectors");
        return collectingSink.getValues();
    }
}
