package dev.release.jobs;

import dev.release.ReleaseInvestigator;
import dev.release.Report;
import dev.release.ToolRuntime;
import dev.release.llm.FakeLlmProvider;
import dev.release.llm.GatewayConfig;
import dev.release.llm.LlmGateway;
import dev.release.llm.LlmProvider;

import java.time.Duration;

/** A durable attempt runs one bounded evidence investigation and optional synthesis. */
public final class InvestigationJobHandler implements JobHandler {
    private final ToolRuntime tools;
    private final LlmGateway gateway = new LlmGateway(new FakeLlmProvider(), GatewayConfig.defaults());
    public InvestigationJobHandler(ToolRuntime tools) { this.tools = tools; }
    @Override public String execute(Job.Claim claim) throws Exception {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        long deadline = System.nanoTime() + claim.spec().budget().toNanos();
        String scenario = claim.spec().scenario().name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
        var report = ReleaseInvestigator.createInvestigation(tools, scenario, claim.spec().budget()).run();
        if (report.state() == Report.State.CANCELLED || Thread.currentThread().isInterrupted()) throw new InterruptedException();
        String text = report.render();
        long remaining = deadline - System.nanoTime();
        if ((report.state() == Report.State.COMPLETED || report.state() == Report.State.PARTIAL) && remaining > 0) {
            var synthesis = gateway.generate(new LlmProvider.Request(text, 256), Duration.ofNanos(remaining));
            text += "LLM synthesis [deterministic fake]: " + synthesis.status() + "\n" + synthesis.text() + "\n";
        }
        // SUCCEEDED means a report was persisted; a report may itself contain a failed/timed-out investigation.
        return text;
    }
}
