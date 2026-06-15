package playground.shared;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Interactive step-by-step mode — pauses between operations.
 * Mirrors the pattern from kubernetes-playground's pkg/prompt/prompt.go
 * and dynamodb-playground's --step flag.
 */
public final class StepPause {

    private final boolean enabled;
    private final BufferedReader reader;

    public StepPause(boolean enabled) {
        this.enabled = enabled;
        this.reader = new BufferedReader(new InputStreamReader(System.in));
    }

    /** Check if step mode is active */
    public boolean isEnabled() {
        return enabled;
    }

    /** Pause and wait for user to press Enter (only if step mode is on) */
    public void pause(String description) {
        if (!enabled) return;

        System.out.println();
        System.out.print("[33m  ⏸  " + description + " — Press Enter to continue...[0m");
        try {
            reader.readLine();
        } catch (IOException e) {
            // If input fails, continue without pausing
            System.out.println();
        }
    }

    /** Pause with a default generic message */
    public void pause() {
        pause("Ready for next step");
    }
}
