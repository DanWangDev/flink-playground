package playground;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import playground.shared.Console;
import playground.shared.StepPause;

import java.util.Map;

/**
 * CLI entry point for the Flink playground.
 *
 * <p>Usage:
 * <pre>{@code
 *   # Run a single exercise (local MiniCluster, no Docker needed)
 *   java -cp target/flink-playground.jar playground.Main --exercise 01 --local --no-step
 *
 *   # Run with interactive step-by-step mode
 *   java -cp target/flink-playground.jar playground.Main --exercise 01 --step
 *
 *   # Run all exercises
 *   java -cp target/flink-playground.jar playground.Main --all --local --no-step
 * }</pre>
 */
public class Main {

    private static final Console log = Console.forModule("main");

    /** Registry of all exercises (id -> class name). Updated as modules are added. */
    private static final Map<String, String> EXERCISE_REGISTRY = Map.ofEntries(
        Map.entry("01", "playground.exercises.Ex01FirstJob"),
        Map.entry("02", "playground.exercises.Ex02KeyedStreams"),
        Map.entry("03", "playground.exercises.Ex03StateManagement"),
        Map.entry("04", "playground.exercises.Ex04Windows"),
        Map.entry("05", "playground.exercises.Ex05EventTime"),
        Map.entry("06", "playground.exercises.Ex06Checkpointing"),
        Map.entry("07", "playground.exercises.Ex07KafkaConnector"),
        Map.entry("08", "playground.exercises.Ex08FileConnectors"),
        Map.entry("09", "playground.exercises.Ex09TableApi"),
        Map.entry("10", "playground.exercises.Ex10FlinkSql"),
        Map.entry("11", "playground.exercises.Ex11ProcessFunction"),
        Map.entry("12", "playground.exercises.Ex12CEP"),
        Map.entry("13", "playground.exercises.Ex13BroadcastState"),
        Map.entry("14", "playground.exercises.Ex14ProductionPatterns"),
        Map.entry("15", "playground.exercises.Ex15AsyncIO"),
        Map.entry("16", "playground.exercises.Ex16StreamJoins"),
        Map.entry("17", "playground.exercises.Ex17Jdbc"),
        Map.entry("18", "playground.exercises.Ex18WindowTVFs")
    );

    public static void main(String[] args) throws Exception {
        int exerciseNumber = 0;
        boolean all = false;
        boolean step = false;
        boolean local = false;

        // Parse CLI arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--exercise":
                case "-e":
                    exerciseNumber = Integer.parseInt(args[++i]);
                    break;
                case "--all":
                case "-a":
                    all = true;
                    break;
                case "--step":
                case "-s":
                    step = true;
                    break;
                case "--local":
                case "-l":
                    local = true;
                    break;
                case "--no-step":
                    step = false;
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        if (!all && exerciseNumber == 0) {
            printUsage();
            System.exit(1);
        }

        String exerciseId = String.format("%02d", exerciseNumber);
        String className = EXERCISE_REGISTRY.get(exerciseId);

        if (className == null) {
            log.error("Exercise " + exerciseId + " not found in registry.");
            log.info("Available exercises: " + EXERCISE_REGISTRY.keySet());
            System.exit(1);
        }

        StepPause stepPause = new StepPause(step);

        StreamExecutionEnvironment env = createEnvironment(local);

        log.header("Flink Playground");
        log.info("Exercise: " + exerciseId);
        log.info("Mode: " + (local ? "MiniCluster (local)" : "Remote cluster"));
        log.info("Step mode: " + (step ? "on" : "off"));

        runExercise(className, env, stepPause);
    }

    private static StreamExecutionEnvironment createEnvironment(boolean local) {
        if (local) {
            return StreamExecutionEnvironment.createLocalEnvironment(1);
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    private static void runExercise(
        String className,
        StreamExecutionEnvironment env,
        StepPause stepPause
    ) throws Exception {
        try {
            Class<?> clazz = Class.forName(className);
            var runner = (playground.shared.ExerciseRunner) clazz.getDeclaredConstructor().newInstance();
            runner.setStepPause(stepPause);
            runner.execute(env);
        } catch (Exception e) {
            log.error("Exercise failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: playground.Main [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --exercise <N>, -e <N>  Run a single exercise (01-14)");
        System.out.println("  --all, -a               Run all exercises");
        System.out.println("  --step, -s              Enable interactive step-by-step mode");
        System.out.println("  --no-step               Disable step mode (default)");
        System.out.println("  --local, -l             Use local MiniCluster (no Docker needed)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -cp target/flink-playground.jar playground.Main -e 01 -l --no-step");
        System.out.println("  java -cp target/flink-playground.jar playground.Main -e 01 --step");
    }
}
