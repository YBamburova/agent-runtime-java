package dev.release;

import java.util.List;

public record Report(State state, List<Outcome> outcomes) {
    public enum State { COMPLETED, PARTIAL, FAILED, TIMED_OUT, CANCELLED }
    public enum Status { SUCCESS, FAILED, TIMED_OUT, CANCELLED, REJECTED }
    public record Outcome(String tool, boolean required, Status status, String detail) {}

    public Report { outcomes = List.copyOf(outcomes); }

    public String render() {
        var text = new StringBuilder("Release Investigator — " + state + "\n");
        text.append("Evidence and source outcomes:\n");
        for (var outcome : outcomes) {
            text.append("- ").append(outcome.tool()).append(outcome.required() ? " [required] " : " [optional] ")
                    .append(outcome.status()).append(": ").append(outcome.detail()).append('\n');
        }
        boolean logs = outcomes.stream().anyMatch(o -> o.tool().equals("logs") && o.status() == Status.SUCCESS);
        boolean deployments = outcomes.stream().anyMatch(o -> o.tool().equals("deployments") && o.status() == Status.SUCCESS);
        text.append("Hypothesis: ");
        if (logs && deployments) {
            text.append("v42's reduced payment timeout may explain checkout failures; compare log and deployment evidence.\n");
        } else {
            text.append("insufficient evidence to connect the release to checkout failures.\n");
        }
        text.append("Limitations: synthetic v42 scenario; correlation does not establish causation.\n")
                .append("Failed, rejected or cancelled sources reduce confidence. Interruption cannot stop arbitrary external work.\n");
        return text.toString();
    }
}
