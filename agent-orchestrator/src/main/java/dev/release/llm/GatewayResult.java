package dev.release.llm;

public record GatewayResult(Status status, String text, int attempts) {
    public enum Status {
        SUCCESS, OVERLOADED, DEADLINE_EXCEEDED, CIRCUIT_OPEN, PROVIDER_FAILED, RETRY_EXHAUSTED, INVALID_REQUEST
    }
}
