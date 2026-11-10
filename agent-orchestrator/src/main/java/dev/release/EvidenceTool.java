package dev.release;

import java.time.Duration;
import java.util.Objects;

/** A blocking boundary. Implementations must close their resources and cooperate with interruption. */
public record EvidenceTool(String name, boolean required, Duration timeout, Call call) {
    @FunctionalInterface
    public interface Call { String gather() throws Exception; }

    public EvidenceTool {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("Tool name is required");
        Investigation.validateBudget(timeout);
        Objects.requireNonNull(call, "call");
    }
}
