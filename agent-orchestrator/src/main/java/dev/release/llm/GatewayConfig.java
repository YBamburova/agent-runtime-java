package dev.release.llm;

import java.time.Duration;

public record GatewayConfig(int maxInFlight, int maxWaiting,
                            int requestsPerWindow, int tokensPerWindow, Duration quotaWindow,
                            int maxAttempts, Duration initialBackoff, Duration maxBackoff,
                            int failuresToOpen, Duration openDuration) {
    public GatewayConfig {
        if (maxInFlight < 1 || maxWaiting < 0 || (long) maxInFlight + maxWaiting > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Invalid capacity limits");
        if (requestsPerWindow < 1 || tokensPerWindow < 1 || failuresToOpen < 1)
            throw new IllegalArgumentException("Quotas and failure threshold must be positive");
        if (maxAttempts < 1 || maxAttempts > 10) throw new IllegalArgumentException("Use 1..10 attempts");
        budgetNanos(quotaWindow); budgetNanos(initialBackoff); budgetNanos(maxBackoff); budgetNanos(openDuration);
        if (initialBackoff.compareTo(maxBackoff) > 0)
            throw new IllegalArgumentException("Maximum backoff must cover initial backoff");
    }

    public static GatewayConfig defaults() {
        return new GatewayConfig(2, 4, 60, 10_000, Duration.ofMinutes(1),
                3, Duration.ofMillis(50), Duration.ofSeconds(1), 3, Duration.ofSeconds(5));
    }

    static long budgetNanos(Duration budget) {
        if (budget == null || budget.isZero() || budget.isNegative() || budget.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Budget must be positive and at most one day");
        return budget.toNanos();
    }
}
