# Milestone 1 verification record

Date: 2026-11-10. This records executed functional checks, not performance measurements.

## Environment and constraints

- macOS 27.0.1, build 26A434, arm64.
- Homebrew OpenJDK 27, build 27, used for compilation and execution.
- Compiler target: `--release 25`. `javap -verbose` confirmed class-file major version 69, minor 0 (Java 25, no preview bytecode).
- Maven 3.10.0, invoked through generated official Maven Wrapper 3.3.4.
- JUnit Jupiter 5.12.2, Compiler Plugin 3.14.1, Surefire 3.5.6, Resources Plugin 3.3.1 and JAR Plugin 3.4.2. These are pinned, documented releases selected from the local cache; they are not claimed to be the newest available versions.
- System default Java was 24.0.2, which cannot build this Java 25 target. Exact JDK 25 was unavailable. Download attempts for JDK 25 and Maven Central failed because shell DNS/network access was restricted.
- The existing read-only Maven cache was copied to `/tmp/release-m2/repository` so Maven could create its own lock files. The cached Maven distribution allowed the wrapper to run without downloading.
- CPU model and RAM inspection through `sysctl` was denied by the sandbox. No benchmark or hardware-dependent performance claim is made.

## Executed commands and results

From the repository root:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home \
  ./mvnw -o -Dmaven.repo.local=/tmp/release-m2/repository verify
```

Result: **BUILD SUCCESS; 14 tests; 0 failures, 0 errors, 0 skipped.** This compiled the application/tests targeting release 25, executed the tests on JDK 27 and packaged an executable JAR with fixtures. The final suite includes 100 cancellation/completion race iterations, a deadline against an uncooperative tool, bounded shutdown observation and expiry while waiting in the tool queue.

The following packaged CLI commands were executed using `/opt/homebrew/opt/openjdk/bin/java`:

```sh
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar healthy
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar partial
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar timeout 200
java -jar target/agent-runtime-java-0.1.0-SNAPSHOT.jar required-failure
```

Observed states respectively: `COMPLETED`, `PARTIAL`, `TIMED_OUT`, `FAILED`. Healthy and partial reports include logs/deployment evidence and the v42 hypothesis; optional dependency failure is explicit. The 200ms deadline retains observed fast evidence and marks the blocked source timed out. Required failure cancels unresolved siblings; which already completed siblings are retained can depend on scheduling.

The official Maven Wrapper plugin was executed offline against the copied cache to generate `mvnw`, `mvnw.cmd` and `.mvn/wrapper/maven-wrapper.properties`. Wrapper invocation itself passed; a fresh uncached wrapper download was not tested.

## Unavailable or deliberately deferred

- Exact JDK 25 compilation/runtime verification: **not run**. Repeat `./mvnw verify` and the four CLI scenarios on JDK 25 before claiming exact-version compatibility.
- Windows wrapper execution: not run.
- CI: not created or executed; planned for Milestone 5.
- Stress workflows, JMH and load measurements: not run; no results exist.
- PostgreSQL concurrency/recovery and real provider integration: not implemented, so not tested.

## Official API and version references

- [Java 25 StructuredTaskScope](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/StructuredTaskScope.html)
- [Java 25 ThreadPoolExecutor](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html)
- [Java 25 FutureTask](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/FutureTask.html)
- [Maven 3.10.0 distribution](https://maven.apache.org/download.cgi)
- [Official Maven Wrapper setup](https://maven.apache.org/tools/wrapper/)
- [Compiler Plugin 3.14.1](https://maven.apache.org/plugins-archives/maven-compiler-plugin-3.14.1/plugin-info.html)
- [Surefire 3.5.6](https://maven.apache.org/surefire-archives/surefire-3.5.6/maven-surefire-plugin/plugin-info.html)
- [JUnit 5.12.2 release notes](https://docs.junit.org/5.12.2/release-notes/index.html)
- [Resources Plugin 3.3.1](https://maven.apache.org/plugins-archives/maven-resources-plugin-3.3.1/plugin-info.html)
- [JAR Plugin 3.4.2](https://maven.apache.org/plugins-archives/maven-jar-plugin-3.4.2/plugin-info.html)

## Interview discussion

1. Why can `FutureTask.cancel(true)` report completion while the external operation is still consuming capacity? Which lifecycle releases capacity here?
2. Where does cancellation linearize relative to report publication, and what happens if successful evidence has already been accepted?
3. Why does a timed structured-concurrency join not guarantee a bounded scope close, and when would structured concurrency be a better fit?
