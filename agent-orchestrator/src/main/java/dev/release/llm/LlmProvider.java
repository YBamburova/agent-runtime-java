package dev.release.llm;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalInt;

/** One blocking transport attempt. Implementations must disable internal retries and honor the budget. */
@FunctionalInterface
public interface LlmProvider {
    Response generate(Request request, Duration remaining) throws Failure, InterruptedException;

    /** Estimate includes input and reserved output tokens; this is not a tokenizer. */
    record Request(String prompt, int estimatedTokens) {
        public Request {
            if (prompt == null || prompt.isBlank() || prompt.length() > 65_536)
                throw new IllegalArgumentException("Prompt must contain 1..65536 characters");
            if (estimatedTokens < 1) throw new IllegalArgumentException("Token estimate must be positive");
        }
    }

    record Response(String text, OptionalInt totalTokens) {
        public Response {
            if (text == null || text.isBlank() || text.length() > 65_536)
                throw new IllegalArgumentException("Response must contain 1..65536 characters");
            Objects.requireNonNull(totalTokens, "totalTokens");
            if (totalTokens.isPresent() && totalTokens.getAsInt() < 0)
                throw new IllegalArgumentException("Usage cannot be negative");
        }
    }

    enum FailureKind { TRANSIENT, PERMANENT }

    /** Typed classification; prompt contents and arbitrary provider messages are not surfaced. */
    final class Failure extends Exception {
        private final FailureKind kind;
        public Failure(FailureKind kind) {
            super(Objects.requireNonNull(kind, "kind").name());
            this.kind = kind;
        }
        public FailureKind kind() { return kind; }
    }
}
