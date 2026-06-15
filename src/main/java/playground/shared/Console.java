package playground.shared;

/**
 * ANSI-colored structured console logger.
 * Mirrors the pattern from kubernetes-playground's pkg/logger/logger.go
 * and dynamodb-playground's shared/logger.ts.
 */
import java.io.Serializable;

public final class Console implements Serializable {

    private static final String RESET = "[0m";
    private static final String BOLD = "[1m";
    private static final String DIM = "[2m";
    private static final String RED = "[31m";
    private static final String GREEN = "[32m";
    private static final String YELLOW = "[33m";
    private static final String BLUE = "[34m";
    private static final String MAGENTA = "[35m";
    private static final String CYAN = "[36m";
    private static final String WHITE = "[37m";

    private final String module;

    private Console(String module) {
        this.module = module;
    }

    /** Factory method: Console.forModule("01-first-job") */
    public static Console forModule(String module) {
        return new Console(module);
    }

    // ── Structural output ──

    /** Major section header */
    public void header(String text) {
        System.out.println();
        System.out.println(BOLD + CYAN + "╔══════════════════════════════════════════════════════════╗" + RESET);
        System.out.println(BOLD + CYAN + "║  " + text + RESET);
        System.out.println(BOLD + CYAN + "╚══════════════════════════════════════════════════════════╝" + RESET);
        System.out.println();
    }

    /** Numbered section within an exercise */
    public void section(String text) {
        System.out.println();
        System.out.println(BOLD + BLUE + "── " + text + " ──" + RESET);
        System.out.println();
    }

    /** Concept explanation */
    public void concept(String text) {
        System.out.println(DIM + "  " + text + RESET);
        System.out.println();
    }

    // ── Status output ──

    public void step(int number, String description) {
        System.out.println(BOLD + WHITE + "[" + number + "] " + description + RESET);
    }

    public void info(String text) {
        System.out.println("  " + CYAN + "ℹ " + text + RESET);
    }

    public void success(String text) {
        System.out.println("  " + GREEN + "✓ " + text + RESET);
    }

    public void warn(String text) {
        System.out.println("  " + YELLOW + "⚠ " + text + RESET);
    }

    public void error(String text) {
        System.out.println("  " + RED + "✗ " + text + RESET);
    }

    // ── Data output ──

    public void command(String text) {
        System.out.println("  " + MAGENTA + "$ " + text + RESET);
    }

    public void output(String text) {
        System.out.println("    " + DIM + text + RESET);
    }

    public void keyValue(String key, String value) {
        System.out.println("  " + BOLD + key + ":" + RESET + " " + value);
    }

    public void separator() {
        System.out.println(DIM + "  ───────────────────────────────────────" + RESET);
    }

    /** Print a data table row */
    public void table(String... columns) {
        StringBuilder sb = new StringBuilder("  ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sb.append(" │ ");
            sb.append(columns[i]);
        }
        System.out.println(sb.toString());
    }

    // ── Summary ──

    public void summary(String text) {
        System.out.println();
        System.out.println(BOLD + GREEN + "  ✓ " + text + RESET);
    }
}
