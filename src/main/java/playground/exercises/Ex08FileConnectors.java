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
        // Write to temp directory (writable in both Docker and local)
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File inputFile = new File(tmpDir, "ex08-input.txt");
        if (inputFile.exists()) inputFile.delete();
        try (FileWriter w = new FileWriter(inputFile)) {
            w.write("record-alpha\nrecord-beta\nrecord-gamma\n");
        }

        log.info("Input file: " + inputFile.getAbsolutePath());

        // File source
        log.section("Step 1: Reading from Files");
        FileSource<String> source = FileSource.forRecordStreamFormat(
            new TextLineInputFormat(), new Path(inputFile.toURI())).build();
        DataStream<String> lines = env.fromSource(source,
            WatermarkStrategy.noWatermarks(), "file-source");

        CollectingSink<String> collectingSink = new CollectingSink<>();
        lines.map(line -> "TRANSFORMED:" + line).returns(String.class)
            .sinkTo(collectingSink);

        env.execute("Exercise 08 — File Connectors");

        // Cleanup
        inputFile.delete();
        return collectingSink.getValues();
    }
}
