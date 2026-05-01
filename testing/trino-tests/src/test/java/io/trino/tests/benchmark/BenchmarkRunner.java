/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.ExceededMemoryLimitException;
import io.trino.Session;
import io.trino.client.FailureException;
import io.trino.execution.Failure;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner.MaterializedResultWithQueryId;
import one.profiler.AsyncProfiler;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

/**
 * Benchmark-agnostic harness driving warmup → measured iterations → optional async-profiler
 * attachment, result validation, and CSV / metadata / merged-collapsed output. Per-benchmark
 * customization lives in {@link Workload} implementations passed to {@link #run}.
 *
 * <p>Subcommands: {@code run} (timed iterations, optional profile, validate vs expected),
 * {@code generate} (invoke the workload's data generator), {@code record} (overwrite the
 * expected-result snapshot).
 */
public final class BenchmarkRunner
{
    private static final Logger log = Logger.get(BenchmarkRunner.class);

    private static final Path PROJECT_ROOT = findProjectRoot();

    public enum ProfileEvent
    {
        NONE,
        /**
         * {@code event=cpu}: perf_events on Linux, itimer fallback on macOS.
         */
        CPU
    }

    /**
     * Execution backend the workload's query runner should use.
     */
    public enum ExecutionMode
    {
        CPU,
        GPU
    }

    /**
     * Per-iteration timing. {@code elapsedMillis} includes planning and queueing;
     * {@code executionMillis} is execution-only.
     */
    public record Measurement(long elapsedMillis, long executionMillis) {}

    /**
     * Leaf or in-stack frames marking idle / parked / polling threads — excluded from the
     * self-time top table (raw flamegraph is unchanged).
     */
    private static final Pattern IDLE_FRAME_PATTERN = Pattern.compile(String.join("|",
            "Parker::park",
            "Unsafe_Park",
            "Unsafe\\.park",
            "LockSupport\\.park",
            "psynch_cvwait",
            "__psynch_cvwait",
            "__psynch_mutexwait",
            "_pthread_cond_wait",
            "semaphore_wait_trap",
            "__semwait_signal",
            "ulock_wait",
            "futex_wait",
            "do_futex",
            "__schedule",
            "ObjectMonitor::wait",
            "ObjectMonitor::enter",
            "os::PlatformEvent::park",
            "epoll_wait",
            "kevent",
            "Selector\\.select",
            "ThreadPoolExecutor\\.getTask",
            "ReservedThreadExecutor",
            "ForkJoinPool\\.awaitWork",
            "ForkJoinPool\\.runWorker",
            "AbstractQueuedSynchronizer.*await"));

    // -Xmx and -Xms are pinned to the same value so the heap doesn't grow during a query.

    private static final String LAUNCHED_MARKER = "benchmark.launched";

    /**
     * Set {@code -Dbenchmark.no.fork=true} to skip the child-JVM launch and run in-process —
     * useful for IDE-attached profilers / debuggers. The parent JVM must then already have
     * {@code --add-modules=jdk.incubator.vector} and a sufficiently sized heap.
     */
    private static final String NO_FORK_SYSTEM_PROPERTY = "benchmark.no.fork";

    private static final int PROFILE_STACK_DEPTH = 512;

    private static final ObjectMapper NDJSON_MAPPER = new ObjectMapper();

    private BenchmarkRunner() {}

    /**
     * Entry point used by per-benchmark {@code main} methods. On first invocation the harness
     * launches a child JVM and forwards its exit code, because Trino needs
     * {@code --add-modules=jdk.incubator.vector} on the boot command line. The launched JVM
     * picks up the rest of its args from the sibling {@code jvm.config}.
     * {@code -Dbenchmark.no.fork=true} disables the fork.
     */
    public static int run(String[] args, Workload workload, Class<?> mainClass)
            throws Exception
    {
        if (!Boolean.getBoolean(LAUNCHED_MARKER) && !Boolean.getBoolean(NO_FORK_SYSTEM_PROPERTY)) {
            return launch(args, workload, mainClass);
        }
        CommandLine cli = new CommandLine(new RootCommand());
        cli.addSubcommand("run", new RunCommand(workload));
        cli.addSubcommand("generate", new GenerateCommand(workload));
        cli.addSubcommand("record", new RecordCommand(workload));
        return cli.execute(args);
    }

    /**
     * Pre-picocli scan for {@code --mode GPU} so we can size the child JVM's heap before fork.
     */
    private static boolean isGpuMode(String[] args)
    {
        for (int i = 0; i < args.length - 1; i++) {
            if (("--mode".equals(args[i]) || "-m".equals(args[i])) && "GPU".equalsIgnoreCase(args[i + 1])) {
                return true;
            }
        }
        return false;
    }

    private static int launch(String[] args, Workload workload, Class<?> mainClass)
            throws Exception
    {
        ExecutionMode mode = isGpuMode(args) ? ExecutionMode.GPU : ExecutionMode.CPU;
        long heapSizeMegabytes = workload.jvmHeapSize(mode).toBytes() / (1024 * 1024);
        String minHeapFlag = "-Xms" + heapSizeMegabytes + "m";
        String maxHeapFlag = "-Xmx" + heapSizeMegabytes + "m";
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add(minHeapFlag);
        command.add(maxHeapFlag);
        command.addAll(readJvmConfig());
        command.add("-D" + LAUNCHED_MARKER + "=true");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass.getName());
        command.addAll(List.of(args));
        log.info("Launching benchmark JVM with %s %s", minHeapFlag, maxHeapFlag);
        return new ProcessBuilder(command).inheritIO().start().waitFor();
    }

    /**
     * Heap sizing is omitted from {@code jvm.config} — {@link #launch} derives it from the workload.
     */
    private static List<String> readJvmConfig()
            throws IOException
    {
        URL url = BenchmarkRunner.class.getResource("jvm.config");
        if (url == null) {
            throw new IllegalStateException("jvm.config not found on classpath next to BenchmarkRunner");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8))) {
            return reader.lines()
                    .map(String::strip)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .toList();
        }
    }

    @Command(
            name = "benchmark",
            mixinStandardHelpOptions = true,
            description = "Run a benchmark, generate its dataset, or record expected results.",
            subcommands = CommandLine.HelpCommand.class)
    static final class RootCommand
            implements Callable<Integer>
    {
        @Spec
        CommandLine.Model.CommandSpec spec;

        @Override
        public Integer call()
        {
            spec.commandLine().usage(System.out);
            return 0;
        }
    }

    @Command(
            name = "run",
            mixinStandardHelpOptions = true,
            description = "Run benchmark queries with optional profiling and result validation.")
    static final class RunCommand
            implements Callable<Integer>
    {
        private final Workload workload;

        @Option(names = {"-S", "--suite-warmup"},
                description = "Number of full passes over the workload's default queries (in order, no validation) before per-query benchmarking. Always runs the entire workload — independent of --query.")
        int suiteWarmup = 1;

        @Option(names = {"-w", "--warmup"}, description = "Number of warmup runs for each query")
        int warmup = 5;

        @Option(names = {"-r", "--runs"}, description = "Number of measured runs for each query")
        int runs = 10;

        @Option(names = {"-q", "--query"}, description = "A specific query number to run (can be repeated)")
        List<Integer> queries = new ArrayList<>();

        @Option(names = {"-p", "--profile"},
                description = "async-profiler event: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
        ProfileEvent profileEvent = ProfileEvent.NONE;

        @Option(names = {"-m", "--mode"},
                description = "Execution backend: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
        ExecutionMode mode = ExecutionMode.CPU;

        @Option(names = "--data", description = "Data directory. Default: workload-specific.")
        Path dataLocation;

        @Option(names = "--debug", description = "Enable debug logging")
        boolean debug;

        RunCommand(Workload workload)
        {
            this.workload = workload;
        }

        @Override
        public Integer call()
                throws Exception
        {
            if (debug) {
                enableDebugLogging();
            }

            Path data = dataLocation != null ? dataLocation : workload.defaultDataLocation();
            if (dataLocation == null) {
                workload.validateDataLocation(data);
            }

            Path benchmarkDataDir = PROJECT_ROOT.resolve("target/benchmark-output").resolve(workload.name());
            Path profileOutputDir = benchmarkDataDir.resolve("profile");

            AsyncProfiler profiler = profileEvent == ProfileEvent.NONE ? null : AsyncProfiler.getInstance();
            if (profiler != null) {
                Files.createDirectories(profileOutputDir);
                log.info("Profiler output will be written to %s", profileOutputDir.toAbsolutePath());
                log.info("Profiler event: %s  interval=%s", profileEvent, workload.profileInterval());
            }

            try (DistributedQueryRunner runner = workload.createRunner(data, mode)) {
                log.info("Running Trino at %s (mode=%s)", runner.getCoordinator().getBaseUrl(), mode);
                log.info("Running %s benchmark: %s suite warmup, %s warmup, %s measured runs, reporting average", workload.name(), suiteWarmup, warmup, runs);
                workload.verifyDataset(runner);
                verifyTableStatistics(runner, workload);

                List<Integer> queriesRun = queries.isEmpty() ? workload.defaultQueries() : List.copyOf(queries);

                List<Integer> suiteQueries = workload.defaultQueries();
                for (int round = 1; round <= suiteWarmup; round++) {
                    log.info("Suite prewarm round %d/%d (%d queries)", round, suiteWarmup, suiteQueries.size());
                    for (int queryNumber : suiteQueries) {
                        log.debug("Starting warmup run of %s", displayName(queryNumber));
                        try {
                            runner.execute(workload.readQuery(queryNumber));
                        }
                        catch (RuntimeException e) {
                            if (!isOutOfMemory(e)) {
                                throw e;
                            }
                            log.warn("Suite prewarm %s: out of memory — continuing", displayName(queryNumber));
                        }
                    }
                }

                if (profiler != null) {
                    writeRunMetadata(profileOutputDir, workload, queriesRun, warmup, runs, profileEvent);
                }

                Map<Integer, List<Measurement>> measurementsByQuery = new LinkedHashMap<>();
                long totalElapsedMillis = 0;
                long totalExecutionMillis = 0;
                for (int queryNumber : queriesRun) {
                    List<Measurement> measurements = benchmarkQuery(runner, queryNumber, profiler, profileOutputDir);
                    measurementsByQuery.put(queryNumber, measurements);
                    if (!measurements.isEmpty()) {
                        totalElapsedMillis += averageMillis(measurements, Measurement::elapsedMillis);
                        totalExecutionMillis += averageMillis(measurements, Measurement::executionMillis);
                    }
                }
                log.info("Sum of averages for all queries: %s ms (execution %s ms)", totalElapsedMillis, totalExecutionMillis);
                writeTimingsCsv(benchmarkDataDir, measurementsByQuery);

                if (profiler != null) {
                    mergeCollapsedFiles(profileOutputDir, queriesRun);
                }
            }

            return 0;
        }

        /**
         * Warmup runs before the profiler attaches so JIT compilation isn't in the hot frames.
         * Each measured iteration's result is compared against the workload's expected NDJSON;
         * mismatch fails the run. Memory-limit failures are logged and the query is skipped
         * (returns an empty list).
         */
        private List<Measurement> benchmarkQuery(DistributedQueryRunner runner, int queryNumber, AsyncProfiler profiler, Path profileOutputDir)
                throws IOException
        {
            String sql = workload.readQuery(queryNumber);
            String displayName = displayName(queryNumber);
            List<String> expectedLines = readExpectedLines(workload, queryNumber);

            List<Measurement> measurements = new ArrayList<>();
            boolean profilerActive = false;
            try {
                for (int i = 0; i < warmup; i++) {
                    log.debug("Starting warmup run of %s", displayName);
                    Measurement measurement = measureAndValidate(runner, sql, displayName, expectedLines);
                    log.debug("Warmup run of %s took %s ms", displayName, measurement.elapsedMillis());
                }
                if (profiler != null) {
                    profiler.execute("start,event=%s,interval=%s,alluser,jstackdepth=%d"
                            .formatted(profileEvent.name().toLowerCase(Locale.ROOT), workload.profileInterval(), PROFILE_STACK_DEPTH));
                    profilerActive = true;
                }
                for (int i = 0; i < runs; i++) {
                    log.debug("Starting measured run of %s", displayName);
                    Measurement measurement = measureAndValidate(runner, sql, displayName, expectedLines);
                    log.debug("Measured run of %s took %s ms", displayName, measurement.elapsedMillis());
                    measurements.add(measurement);
                }
            }
            catch (RuntimeException e) {
                if (profilerActive) {
                    try {
                        profiler.execute("stop");
                    }
                    catch (RuntimeException stopError) {
                        log.warn(stopError, "Profiler stop failed after %s error", displayName);
                    }
                }
                if (isOutOfMemory(e)) {
                    log.warn("%s: FAILED (out of memory) — skipping query", displayName);
                    return List.of();
                }
                throw e;
            }

            if (profilerActive) {
                Path snapshot = Files.createTempFile("benchmark-" + displayName + "-", ".collapsed");
                Path htmlFile = profileOutputDir.resolve(displayName + ".html");
                profiler.execute("dump,file=%s,collapsed,threads".formatted(snapshot.toAbsolutePath()));
                profiler.execute("stop,file=%s,flamegraph,threads".formatted(htmlFile.toAbsolutePath()));
                long kept = postProcessProfile(profileOutputDir, displayName, snapshot, profileEvent, workload);
                Files.delete(snapshot);
                log.info("Profiler output for %s post-processed (%d samples kept after idle filter)", displayName, kept);
            }

            long averageElapsed = averageMillis(measurements, Measurement::elapsedMillis);
            long averageExecution = averageMillis(measurements, Measurement::executionMillis);
            log.info("Average for %s: %s ms (execution %s ms)", displayName, averageElapsed, averageExecution);
            return List.copyOf(measurements);
        }
    }

    private static boolean isOutOfMemory(Throwable error)
    {
        String oomType = ExceededMemoryLimitException.class.getName();
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof ExceededMemoryLimitException) {
                return true;
            }
            if (current instanceof Failure failure
                    && oomType.equals(failure.getFailureInfo().type())) {
                return true;
            }
            if (current instanceof FailureException failureException
                    && oomType.equals(failureException.getFailureInfo().getType())) {
                return true;
            }
        }
        return false;
    }

    @Command(
            name = "generate",
            mixinStandardHelpOptions = true,
            description = "Generate the benchmark dataset into the data directory (workload-specific).")
    static final class GenerateCommand
            implements Callable<Integer>
    {
        private final Workload workload;

        @Option(names = "--data", description = "Target directory. Default: workload-specific.")
        Path dataLocation;

        GenerateCommand(Workload workload)
        {
            this.workload = workload;
        }

        @Override
        public Integer call()
                throws Exception
        {
            Path target = dataLocation != null ? dataLocation : workload.defaultDataLocation();
            log.info("Generating %s data into %s", workload.name(), target);
            workload.generateData(target);
            log.info("Data generation complete.");
            return 0;
        }
    }

    @Command(
            name = "record",
            mixinStandardHelpOptions = true,
            description = "Run each query once and write its result to the workload's expected-results file.")
    static final class RecordCommand
            implements Callable<Integer>
    {
        private final Workload workload;

        @Option(names = {"-q", "--query"}, description = "A specific query number (can be repeated)")
        List<Integer> queries = new ArrayList<>();

        @Option(names = "--data", description = "Data directory. Default: workload-specific.")
        Path dataLocation;

        RecordCommand(Workload workload)
        {
            this.workload = workload;
        }

        @Override
        public Integer call()
                throws Exception
        {
            Path data = dataLocation != null ? dataLocation : workload.defaultDataLocation();
            if (dataLocation == null) {
                workload.validateDataLocation(data);
            }
            try (DistributedQueryRunner runner = workload.createRunner(data, ExecutionMode.CPU)) {
                workload.verifyDataset(runner);
                verifyTableStatistics(runner, workload);
                List<Integer> queriesRun = queries.isEmpty() ? workload.defaultQueries() : List.copyOf(queries);
                for (int queryNumber : queriesRun) {
                    Path target = recordTargetFor(workload.expectedResultResource(queryNumber));
                    String sql = workload.readQuery(queryNumber);
                    MaterializedResult result = runner.execute(sql);
                    Files.createDirectories(target.getParent());
                    writeNdjson(target, result);
                    log.info("Recorded %s -> %s (%d rows)", displayName(queryNumber), target, result.getRowCount());
                }
            }
            return 0;
        }
    }

    private static Measurement measureAndValidate(
            DistributedQueryRunner runner,
            String sql,
            String displayName,
            List<String> expectedLines)
    {
        MaterializedResultWithQueryId withId = runner.executeWithQueryId(runner.getDefaultSession(), sql);
        var stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(withId.queryId()).getQueryStats();
        List<String> actualLines = formatNdjsonLines(withId.result());
        if (!actualLines.equals(expectedLines)) {
            throw new AssertionError(formatResultMismatch(displayName, expectedLines, actualLines));
        }
        return new Measurement(stats.getElapsedTime().toMillis(), stats.getExecutionTime().toMillis());
    }

    /**
     * Strip idle/parked stacks from the collapsed snapshot and emit {@code .flat.txt}
     * (top-30 leaf frames, thread-group breakdown) and {@code .filtered.collapsed} (consumed
     * by {@link #mergeCollapsedFiles}).
     */
    private static long postProcessProfile(
            Path profileOutputDir,
            String displayName,
            Path cumulativeSnapshot,
            ProfileEvent profileEvent,
            Workload workload)
            throws IOException
    {
        Map<String, Long> selfTime = new HashMap<>();
        Map<String, Long> byThreadGroup = new HashMap<>();
        long totalSamples = 0;
        long keptSamples = 0;

        Path filteredFile = profileOutputDir.resolve(displayName + ".filtered.collapsed");
        try (BufferedWriter filtered = Files.newBufferedWriter(filteredFile, UTF_8)) {
            for (String line : Files.readAllLines(cumulativeSnapshot, UTF_8)) {
                int lastSpace = line.lastIndexOf(' ');
                if (lastSpace < 0) {
                    throw new IllegalStateException("Unexpected collapsed-profile line (no space separator): " + line);
                }
                long count;
                try {
                    count = Long.parseLong(line.substring(lastSpace + 1).trim());
                }
                catch (NumberFormatException e) {
                    throw new IllegalStateException("Unexpected collapsed-profile line (non-integer count): " + line, e);
                }
                totalSamples += count;
                String stack = line.substring(0, lastSpace);
                int lastSemicolon = stack.lastIndexOf(';');
                String leaf = lastSemicolon < 0 ? stack : stack.substring(lastSemicolon + 1);
                if (IDLE_FRAME_PATTERN.matcher(leaf).find() || IDLE_FRAME_PATTERN.matcher(stack).find()) {
                    continue;
                }
                keptSamples += count;
                filtered.write(line);
                filtered.newLine();
                selfTime.merge(leaf, count, Long::sum);
                byThreadGroup.merge(threadGroup(stack), count, Long::sum);
            }
        }

        StringBuilder flat = new StringBuilder(4096);
        flat.append(format("# Flat self-time top-30 for %s (idle / parked stacks excluded)%n", displayName));
        flat.append(format("# Raw samples: %d  Kept after filter: %d (%.1f%%)%n",
                totalSamples, keptSamples, 100.0 * keptSamples / Math.max(1, totalSamples)));
        flat.append(format("# Event: %s  Interval: %s%n", profileEvent.name().toLowerCase(Locale.ROOT), workload.profileInterval()));
        flat.append(format("#%n"));
        flat.append(format("# By thread group (after filter):%n"));
        long keptForLambda = keptSamples;
        byThreadGroup.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(entry -> flat.append(format("#   %-16s %8d  %5.1f%%%n",
                        entry.getKey(), entry.getValue(), 100.0 * entry.getValue() / Math.max(1, keptForLambda))));
        flat.append(format("#%n"));
        flat.append(format("%-10s %-8s %s%n", "samples", "pct", "leaf frame"));
        selfTime.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(30)
                .forEach(entry -> flat.append(format("%-10d %-7.2f%% %s%n",
                        entry.getValue(), 100.0 * entry.getValue() / Math.max(1, keptForLambda), entry.getKey())));

        Files.writeString(profileOutputDir.resolve(displayName + ".flat.txt"), flat.toString(), UTF_8);
        return keptSamples;
    }

    /**
     * Concatenate per-query collapsed files into one cross-query file and delete the inputs.
     */
    private static void mergeCollapsedFiles(Path profileOutputDir, List<Integer> queriesRun)
            throws IOException
    {
        Path merged = profileOutputDir.resolve("merged.collapsed");
        List<Path> consumed = new ArrayList<>(queriesRun.size());
        try (OutputStream out = Files.newOutputStream(merged)) {
            for (int queryNumber : queriesRun) {
                Path perQueryFile = profileOutputDir.resolve("%s.filtered.collapsed".formatted(displayName(queryNumber)));
                if (!Files.exists(perQueryFile)) {
                    continue;
                }
                Files.copy(perQueryFile, out);
                consumed.add(perQueryFile);
            }
        }
        for (Path perQueryFile : consumed) {
            Files.deleteIfExists(perQueryFile);
        }
        log.info("Merged collapsed stacks written to %s (%d per-query files consumed and deleted)",
                merged, consumed.size());
    }

    /**
     * Write per-query timing data to {@code timings.csv} (overwrites any previous file —
     * copy or rename it to keep history across runs).
     */
    private static void writeTimingsCsv(
            Path benchmarkDataDir,
            Map<Integer, List<Measurement>> measurementsByQuery)
            throws IOException
    {
        if (measurementsByQuery.isEmpty()) {
            return;
        }
        int maxMeasurements = measurementsByQuery.values().stream().mapToInt(List::size).max().orElse(0);
        Path csvFile = benchmarkDataDir.resolve("timings.csv");
        Files.createDirectories(csvFile.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(csvFile, UTF_8)) {
            writer.write("query");
            for (int i = 1; i <= maxMeasurements; i++) {
                writer.write(",run_%d_elapsed,run_%d_execution".formatted(i, i));
            }
            writer.write(",average_elapsed_ms,average_execution_ms,n_measurements");
            writer.newLine();
            for (Map.Entry<Integer, List<Measurement>> entry : measurementsByQuery.entrySet()) {
                writer.write(displayName(entry.getKey()));
                List<Measurement> measurements = entry.getValue();
                for (int i = 0; i < maxMeasurements; i++) {
                    if (i < measurements.size()) {
                        writer.write(",%d,%d".formatted(measurements.get(i).elapsedMillis(), measurements.get(i).executionMillis()));
                    }
                    else {
                        writer.write(",,");
                    }
                }
                if (measurements.isEmpty()) {
                    writer.write(",,,0");
                }
                else {
                    long averageElapsed = averageMillis(measurements, Measurement::elapsedMillis);
                    long averageExecution = averageMillis(measurements, Measurement::executionMillis);
                    writer.write(",%d,%d,%d".formatted(averageElapsed, averageExecution, measurements.size()));
                }
                writer.newLine();
            }
        }
        log.info("Wrote per-query timings to %s", csvFile);
    }

    private static void writeRunMetadata(
            Path profileOutputDir,
            Workload workload,
            List<Integer> queriesRun,
            int warmup,
            int runs,
            ProfileEvent profileEvent)
            throws IOException
    {
        List<String> jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        Runtime runtime = Runtime.getRuntime();
        String content = """
                         # Benchmark run metadata (workload: %s)
                         timestamp:               %s
                         warmup runs:             %d
                         measured runs:           %d
                         queries:                 %s
                         profile event:           %s
                         profile interval:        %s
                         profile idle filter:     %s
                         java version:            %s
                         java vendor:             %s
                         os.name:                 %s
                         os.arch:                 %s
                         heap max (-Xmx):         %d MB
                         available processors:    %d
                         preserve frame pointer:  %s
                         jvm input arguments:
                         %s
                         """.formatted(
                workload.name(),
                Instant.now(),
                warmup,
                runs,
                queriesRun.toString(),
                profileEvent,
                workload.profileInterval(),
                IDLE_FRAME_PATTERN.pattern(),
                System.getProperty("java.version"),
                System.getProperty("java.vendor"),
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                runtime.maxMemory() / (1024 * 1024),
                runtime.availableProcessors(),
                jvmArguments.stream().anyMatch(argument -> argument.equals("-XX:+PreserveFramePointer")),
                jvmArguments.stream().map(argument -> "  " + argument).collect(joining("\n")));
        Files.writeString(profileOutputDir.resolve("run.meta.txt"), content, UTF_8);
    }

    // async-profiler's `threads` flag in collapsed output does NOT prefix the stack with the
    // thread name (verified empirically — the bottom frame is the OS-level entry like
    // `thread_start` or `java/lang/Thread.run`, same for every thread). So we classify by
    // matching characteristic frames anywhere in the stack instead. Order matters: the first
    // match wins, so place narrower patterns above broader ones.
    private record ThreadGroupRule(String label, Pattern pattern) {}

    private static final List<ThreadGroupRule> THREAD_GROUP_RULES = List.of(
            new ThreadGroupRule("task-runner",
                    Pattern.compile("TimeSharingTaskExecutor\\$TaskRunner|PrioritizedSplitRunner")),
            new ThreadGroupRule("compiler",
                    Pattern.compile("CompileBroker::compiler_thread_loop|C1Compiler|C2Compiler")),
            new ThreadGroupRule("gc-vm",
                    Pattern.compile("G1ConcurrentRefine|G1ParScanThreadState|GCTaskThread|VMThread::run")),
            new ThreadGroupRule("jetty",
                    Pattern.compile("ServerConnector|HttpChannelOverHttp|ReservedThread")),
            new ThreadGroupRule("jvm-internal",
                    Pattern.compile("Reference\\$ReferenceHandler|Finalizer\\$FinalizerThread")));

    private static String threadGroup(String stack)
    {
        for (ThreadGroupRule rule : THREAD_GROUP_RULES) {
            if (rule.pattern.matcher(stack).find()) {
                return rule.label;
            }
        }
        return "other";
    }

    private static long averageMillis(List<Measurement> measurements, ToLongFunction<Measurement> field)
    {
        return (long) measurements.stream().mapToLong(field).average().orElseThrow();
    }

    private static List<String> readExpectedLines(Workload workload, int queryNumber)
            throws IOException
    {
        String resource = workload.expectedResultResource(queryNumber);
        URL url = BenchmarkRunner.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new IllegalStateException(
                    "Expected result resource not on classpath: " + resource + ". Run `record` first to generate it.");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8))) {
            return reader.lines().toList();
        }
    }

    /**
     * Resolves a resource path under the source tree so {@code record}'s output is picked up by the next build.
     */
    private static Path recordTargetFor(String resource)
    {
        return PROJECT_ROOT.resolve("testing/trino-benchmark-queries/src/main/resources").resolve(resource);
    }

    /**
     * Verify each declared table has table-level row count and per-column NDV statistics,
     * triggering {@code ANALYZE} if not — stat-less or partial-stat plans aren't representative
     * of real behavior.
     */
    private static void verifyTableStatistics(DistributedQueryRunner runner, Workload workload)
    {
        List<String> tables = workload.tablesForStats();
        log.info("Verifying table statistics for %d table(s): %s", tables.size(), tables);
        for (String table : tables) {
            Optional<String> missing = findMissingStatistic(runner, table);
            if (missing.isEmpty()) {
                log.info("  %s: statistics present", table);
                continue;
            }
            log.info("  %s: missing %s, running ANALYZE", table, missing.get());
            long start = System.nanoTime();
            runner.execute("ANALYZE " + table);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            log.info("  %s: ANALYZE completed in %d ms", table, elapsedMillis);
        }
    }

    private static Optional<String> findMissingStatistic(DistributedQueryRunner runner, String table)
    {
        MaterializedResult result = runner.execute("SHOW STATS FOR " + table);
        // SHOW STATS columns: column_name(0), data_size(1), distinct_values_count(2),
        // nulls_fraction(3), row_count(4), low_value(5), high_value(6). The summary row has a
        // null column_name and carries the table-level row_count; per-column rows carry NDV.
        boolean hasRowCount = false;
        for (MaterializedRow row : result.getMaterializedRows()) {
            if (row.getField(0) == null) {
                hasRowCount = row.getField(4) != null;
            }
            else if (row.getField(2) == null) {
                return Optional.of("distinct_values_count for column " + row.getField(0));
            }
        }
        return hasRowCount ? Optional.empty() : Optional.of("row_count");
    }

    /**
     * Render a {@link MaterializedResult} as NDJSON (one JSON array per row, in result order).
     * Non-JSON-native cell types (timestamps, dates, BigDecimal) are stringified.
     */
    private static List<String> formatNdjsonLines(MaterializedResult result)
    {
        List<String> lines = new ArrayList<>(result.getRowCount());
        for (MaterializedRow row : result.getMaterializedRows()) {
            List<Object> jsonReady = new ArrayList<>(row.getFieldCount());
            for (int i = 0; i < row.getFieldCount(); i++) {
                jsonReady.add(toJsonValue(row.getField(i)));
            }
            try {
                lines.add(NDJSON_MAPPER.writeValueAsString(jsonReady));
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to serialize row to NDJSON", e);
            }
        }
        return lines;
    }

    private static void writeNdjson(Path target, MaterializedResult result)
            throws IOException
    {
        try (BufferedWriter writer = Files.newBufferedWriter(target, UTF_8)) {
            for (String line : formatNdjsonLines(result)) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    private static Object toJsonValue(Object cell)
    {
        if (cell == null || cell instanceof Boolean || cell instanceof Number || cell instanceof String) {
            return cell;
        }
        return cell.toString();
    }

    private static String formatResultMismatch(String displayName, List<String> expected, List<String> actual)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Result mismatch for ").append(displayName)
                .append(": expected ").append(expected.size())
                .append(" rows, got ").append(actual.size()).append('\n');
        int max = Math.max(expected.size(), actual.size());
        int shown = 0;
        for (int i = 0; i < max && shown < 10; i++) {
            String e = i < expected.size() ? expected.get(i) : "<missing>";
            String a = i < actual.size() ? actual.get(i) : "<missing>";
            if (!e.equals(a)) {
                builder.append("  row ").append(i).append(":\n");
                builder.append("    - expected: ").append(e).append('\n');
                builder.append("    + actual:   ").append(a).append('\n');
                shown++;
            }
        }
        return builder.toString();
    }

    /**
     * Compare a sorted {@code <size> <relative-path>} listing of {@code dataLocation} against an
     * expected snapshot. Forward-slash separators keep checked-in expected strings portable.
     */
    public static void verifyDataListing(Path dataLocation, String hydrationHint, String expected)
    {
        List<String> actualLines;
        try (Stream<Path> files = Files.walk(dataLocation)) {
            actualLines = files
                    .filter(Files::isRegularFile)
                    .map(path -> "%d %s".formatted(
                            fileSize(path),
                            dataLocation.relativize(path).toString().replace(File.separatorChar, '/')))
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String actual = actualLines.stream().sorted().map(line -> line + "\n").collect(joining(""));
        if (actual.equals(expected)) {
            return;
        }
        Set<String> expectedSet = expected.lines().collect(Collectors.toSet());
        Set<String> actualSet = actual.lines().collect(Collectors.toSet());
        String diff = Stream.concat(
                        expectedSet.stream().filter(line -> !actualSet.contains(line)).map(line -> "- " + line),
                        actualSet.stream().filter(line -> !expectedSet.contains(line)).map(line -> "+ " + line))
                .sorted()
                .collect(joining("\n"));
        throw new IllegalStateException("Data location %s is not up to date. %s\nDiff (- expected, + actual):\n%s"
                .formatted(dataLocation, hydrationHint, diff));
    }

    private static long fileSize(Path path)
    {
        try {
            return Files.size(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Pre-configured Hive runner builder with deterministic-write settings (single writer per
     * task, no scaling, snappy parquet). Workloads chain their own builder calls and {@code build}.
     */
    public static HiveQueryRunner.Builder<?> dataGenerationBuilder()
    {
        return HiveQueryRunner.builder()
                .setWorkerCount(0)
                .setSkipTimezoneSetup(true)
                .addExtraProperties(ImmutableMap.<String, String>builder()
                        .put("scale-writers", "false")
                        .put("task.scale-writers.enabled", "false")
                        .put("task.max-writer-count", "1")
                        .put("query.max-writer-task-count", "1")
                        .put("redistribute-writes", "false")
                        .buildOrThrow())
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .addHiveProperty("hive.metastore.disable-location-checks", "true")
                .addHiveProperty("hive.compression-codec", "SNAPPY");
    }

    /**
     * Pin the given session to a single writer task. {@code TestingTrinoServer} hardcodes
     * {@code task.concurrency=4} and {@code task.min-writer-count=2}, neither of which can be
     * overridden via extra properties — they have to be set as session properties.
     */
    public static Session withSingleWriter(Session session)
    {
        return Session.builder(session)
                .setSystemProperties(ImmutableMap.<String, String>builder()
                        .put("task_concurrency", "1")
                        .put("task_min_writer_count", "1")
                        .buildOrThrow())
                .build();
    }

    /**
     * Strip the {@code .crc} sidecars Hadoop's {@code RawLocalFileSystem} drops next to every
     * written parquet file, so the dataset matches the workload's expected file listing.
     */
    public static void cleanCrcFiles(Path target)
            throws IOException
    {
        try (Stream<Path> walk = Files.walk(target)) {
            walk.filter(path -> path.getFileName().toString().endsWith(".crc"))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    /**
     * Apply mode-specific extras (GPU acceleration toggles, GPU-tuned Hive split sizes).
     */
    public static void applyExecutionMode(HiveQueryRunner.Builder<?> builder, ExecutionMode mode)
    {
        switch (mode) {
            case CPU -> builder.addExtraProperty("gpu-acceleration.enabled", "false");
            case GPU -> builder
                    .addExtraProperty("gpu-acceleration.enabled", "true")
                    .addExtraProperty("gpu-acceleration.table-scan-enabled", "true")
                    // Larger splits than the 64 MB default give a substantial GPU Parquet decode
                    // speedup. TODO reconsider raising further (e.g. 512 MB).
                    .addHiveProperty("hive.max-initial-split-size", "256MB")
                    .addHiveProperty("hive.max-split-size", "256MB");
        }
    }

    static void enableDebugLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.spi.gpu", Level.DEBUG);
        logging.setLevel("io.trino.operator.gpu", Level.DEBUG);
        logging.setLevel("io.trino.tests.benchmark", Level.DEBUG);
    }

    /**
     * Nearest {@code .git} ancestor of {@code user.dir} as the project root, or {@code user.home}
     * when running from a packaged jar without a source tree (so output still lands somewhere
     * predictable). Only the {@code record} subcommand needs the source tree.
     */
    private static Path findProjectRoot()
    {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        for (Path p = current; p != null; p = p.getParent()) {
            if (Files.exists(p.resolve(".git"))) {
                return p;
            }
        }
        Path home = Path.of(System.getProperty("user.home"));
        log.warn("No .git ancestor of %s; falling back to user.home (%s) as project root", current, home);
        return home;
    }

    private static String displayName(int queryNumber)
    {
        return "q%02d".formatted(queryNumber);
    }
}
