package dev.release;

import dev.release.llm.FakeLlmProvider;
import dev.release.llm.GatewayConfig;
import dev.release.llm.GatewayDemo;
import dev.release.llm.LlmGateway;
import dev.release.llm.LlmProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class ReleaseInvestigator {
    private ReleaseInvestigator() {}

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("jobs")) {
            dev.release.jobs.JobsCli.run(java.util.Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (args.length > 2) throw new IllegalArgumentException("Usage: [healthy|partial|timeout|required-failure] [deadline-ms], or overload");
        String scenario = args.length == 0 ? "healthy" : args[0];
        if (scenario.equals("overload")) {
            if (args.length != 1) throw new IllegalArgumentException("Usage: overload");
            GatewayDemo.runOverload();
            return;
        }
        if (!List.of("healthy", "partial", "timeout", "required-failure").contains(scenario))
            throw new IllegalArgumentException("Unknown scenario: " + scenario);
        long millis = args.length < 2 ? 20_000 : Long.parseLong(args[1]);
        try (var runtime = new ToolRuntime(3, 3)) {
            var investigation = createInvestigation(runtime, scenario, Duration.ofMillis(millis));
            long deadline = System.nanoTime() + Duration.ofMillis(millis).toNanos();
            Thread hook = new Thread(investigation::cancel, "cancel-investigation");
            Runtime.getRuntime().addShutdownHook(hook);
            try {
                var report = investigation.run();
                System.out.print(report.render());
                long remaining = deadline - System.nanoTime();
                if ((report.state() == Report.State.COMPLETED || report.state() == Report.State.PARTIAL) && remaining > 0) {
                    var gateway = new LlmGateway(new FakeLlmProvider(), GatewayConfig.defaults());
                    var synthesis = gateway.generate(new LlmProvider.Request(report.render(), 256), Duration.ofNanos(remaining));
                    System.out.printf("LLM synthesis [deterministic fake]: %s, attempts=%d%n%s%n",
                            synthesis.status(), synthesis.attempts(), synthesis.text());
                } else {
                    System.out.println("LLM synthesis skipped: investigation ended or its deadline expired.");
                }
            }
            finally { Runtime.getRuntime().removeShutdownHook(hook); }
        }
    }

    public static Investigation createInvestigation(ToolRuntime runtime, String scenario, Duration budget) {
        if (scenario == null || !List.of("healthy", "partial", "timeout", "required-failure").contains(scenario))
            throw new IllegalArgumentException("Unknown scenario");
        return new Investigation(runtime, List.of(
                new EvidenceTool("logs", true, Duration.ofSeconds(5), () -> {
                    if (scenario.equals("required-failure")) throw new IOException("Synthetic failure");
                    return fixture("logs");
                }),
                new EvidenceTool("deployments", true, Duration.ofSeconds(5), () -> fixture("deployments")),
                new EvidenceTool("dependencies", false, Duration.ofSeconds(5), () -> {
                    if (scenario.equals("partial")) throw new IOException("Synthetic failure");
                    if (scenario.equals("timeout")) new CountDownLatch(1).await();
                    return fixture("dependencies");
                })), budget);
    }

    private static String fixture(String name) throws IOException {
        try (var stream = ReleaseInvestigator.class.getResourceAsStream("/fixtures/" + name + ".txt")) {
            if (stream == null) throw new IOException("Missing fixture");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8).strip();
        }
    }
}
