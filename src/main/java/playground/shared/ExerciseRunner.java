package playground.shared;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * Abstract base class for all Flink exercises.
 *
 * <p>Each exercise extends this and implements the run method.
 * Pattern mirrors kubernetes-playground exercises and dynamodb-playground exercises.</p>
 */
public abstract class ExerciseRunner {

    protected final String id;
    protected final String title;
    protected final Console log;
    protected StepPause step;

    protected ExerciseRunner(String id, String title) {
        this.id = id;
        this.title = title;
        this.log = Console.forModule(id);
        this.step = new StepPause(false);  // no-op by default, overridden in CLI mode
    }

    /** Enable step-by-step interactive mode */
    public void setStepPause(StepPause step) {
        this.step = step;
    }

    /** Get the exercise identifier */
    public String getId() {
        return id;
    }

    /** Get the exercise title */
    public String getTitle() {
        return title;
    }

    /**
     * Run the exercise pipeline.
     *
     * @param env the Flink execution environment
     * @return list of collected results for test assertions
     * @throws Exception if the pipeline fails
     */
    public abstract List<?> run(StreamExecutionEnvironment env) throws Exception;

    /** Execute the pipeline, print header/footer, and return results */
    public List<?> execute(StreamExecutionEnvironment env) throws Exception {
        log.header("Exercise " + id + ": " + title);

        List<?> results = run(env);

        log.summary("Exercise " + id + " completed successfully.");
        return results;
    }
}
