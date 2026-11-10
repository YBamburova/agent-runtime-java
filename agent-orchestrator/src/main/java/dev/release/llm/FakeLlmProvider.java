package dev.release.llm;

import java.time.Duration;
import java.util.OptionalInt;

/** Deliberately transparent fixture synthesis, not a simulation of model quality or tokenization. */
public final class FakeLlmProvider implements LlmProvider {
    @Override public Response generate(Request request, Duration remaining) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        GatewayConfig.budgetNanos(remaining);
        boolean logs = request.prompt().contains("logs [required] SUCCESS:");
        boolean deployments = request.prompt().contains("deployments [required] SUCCESS:");
        String text = logs && deployments
                ? "Compare v42's 500ms payment timeout with the observed 850ms payment latency. "
                    + "A timeout regression is plausible; validate with rollback or a controlled replay."
                : "Evidence is incomplete. Restore the missing required sources before attributing failures to v42.";
        return new Response(text, OptionalInt.of(64));
    }
}
