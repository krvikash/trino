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

import ai.rapids.cudf.Rmm;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Key;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.ExceededMemoryLimitException;
import io.trino.Session;
import io.trino.blob.cache.alluxio.AlluxioBlobCachePlugin;
import io.trino.client.FailureException;
import io.trino.client.FailureInfo;
import io.trino.execution.QueryInfo;
import io.trino.execution.TaskManagerConfig;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.NodeVersion;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
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
import java.time.Duration;
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
import java.util.concurrent.ExecutionException;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.GPU_EXECUTION_ENABLED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
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
        CPU,
        WALL,
    }

    /**
     * Execution backend the workload's query runner should use.
     */
    public enum ExecutionMode
    {
        CPU,
        GPU,
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
    private static final Pattern IDLE_FRAME_PATTERN = Pattern.compile(String.join(
            "|",
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
            // glibc/Linux park primitives (the macOS-named entries above miss these).
            "__futex_abstimed_wait",
            "pthread_cond_wait",
            "pthread_cond_timedwait",
            "sem_wait",
            "pthread_join",
            "pthread_clockjoin",
            "__schedule",
            "ObjectMonitor::wait",
            "ObjectMonitor::enter",
            "os::PlatformEvent::park",
            "epoll_wait",
            "kevent",
            "Selector\\.select",
            // native NIO accept/poll idle loops (jetty / RPC server threads).
            "__libc_accept",
            "Java_sun_nio_ch_Net_accept",
            "Java_sun_nio_ch_Net_poll",
            "ThreadPoolExecutor\\.getTask",
            "ForkJoinPool\\.awaitWork",
            "AbstractQueuedSynchronizer.*await",
            // threads frozen at a safepoint poll (not executing); matched above the generic syscall leaf.
            "SafepointSynchronize::block",
            "LinuxWaitBarrier::wait",
            // scheduler wake-up signaling (waking a sleeping worker, not query work).
            "Unsafe_Unpark",
            "Unsafe\\.unpark",
            "LockSupport\\.unpark",
            "ForkJoinPool\\.signalWork"));

    // -Xmx and -Xms are pinned to the same value so the heap doesn't grow during a query.

    private static final String LAUNCHED_MARKER = "benchmark.launched";

    /**
     * Set {@code -Dbenchmark.no.fork=true} to skip the child-JVM launch and run in-process —
     * useful for IDE-attached profilers / debuggers. The parent JVM must then already have
     * {@code --add-modules=jdk.incubator.vector} and a sufficiently sized heap.
     */
    private static final String NO_FORK_SYSTEM_PROPERTY = "benchmark.no.fork";

    private static final int PROFILE_STACK_DEPTH = 512;

    private static final String FS_CACHE_MAX_SIZE = "200GB";

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
        Launcher launcher = new Launcher(args, workload, mainClass);
        CommandLine cli = new CommandLine(new RootCommand());
        cli.addSubcommand("run", new RunCommand(launcher, workload));
        cli.addSubcommand("runner", new QueryRunnerCommand(launcher, workload));
        cli.addSubcommand("generate", new GenerateCommand(launcher, workload));
        cli.addSubcommand("record", new RecordCommand(launcher, workload));
        return cli.execute(args);
    }

    private static class Launcher
    {
        private final String[] originalArgs;
        private final Workload workload;
        private final Class<?> mainClass;

        public Launcher(String[] originalArgs, Workload workload, Class<?> mainClass)
        {
            this.originalArgs = originalArgs.clone();
            this.workload = requireNonNull(workload, "workload is null");
            this.mainClass = requireNonNull(mainClass, "mainClass is null");
        }

        boolean relaunchIfNeeded(Optional<List<String>> launchWrapperCommand)
                throws Exception
        {
            if (Boolean.getBoolean(LAUNCHED_MARKER) || Boolean.getBoolean(NO_FORK_SYSTEM_PROPERTY)) {
                return false;
            }
            int exitCode = launch(launchWrapperCommand, originalArgs, workload, mainClass);
            checkState(exitCode == 0, "Child process exited with %s", exitCode);
            return true;
        }
    }

    private static int launch(Optional<List<String>> launchWrapperCommand, String[] args, Workload workload, Class<?> mainClass)
            throws Exception
    {
        long heapSizeMegabytes = workload.jvmHeapSize().toBytes() / (1024 * 1024);
        String minHeapFlag = "-Xms" + heapSizeMegabytes + "m";
        String maxHeapFlag = "-Xmx" + heapSizeMegabytes + "m";
        List<String> command = new ArrayList<>();
        launchWrapperCommand.ifPresent(command::addAll);
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add(minHeapFlag);
        command.add(maxHeapFlag);
        command.addAll(readJvmConfig());
        command.add("-D" + LAUNCHED_MARKER + "=true");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass.getName());
        command.addAll(List.of(args));
        if (launchWrapperCommand.isPresent()) {
            log.info("Launching benchmark JVM under %s with %s %s", String.join(" ", launchWrapperCommand.get()), minHeapFlag, maxHeapFlag);
        }
        else {
            log.info("Launching benchmark JVM with %s %s", minHeapFlag, maxHeapFlag);
        }
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
        private final Launcher launcher;
        private final Workload workload;

        @Option(names = {"-W", "--suite-warmup"},
                description = "Number of full passes over the workload's default queries (in order, no validation) before per-query benchmarking. Always runs the entire workload — independent of --query.")
        int suiteWarmup = 1;

        @Option(names = {"-w", "--warmup"}, description = "Number of warmup runs for each query")
        int warmup = 5;

        @Option(names = {"-r", "--runs"}, description = "Number of measured runs for each query")
        int runs = 10;

        @Option(names = {"-q", "--query"}, description = "A specific query number to run (can be repeated)")
        List<String> queries = new ArrayList<>();

        @Option(names = {"-s", "--skip-query"}, description = "A query number to skip; recorded as all-zero timings in the CSV (can be repeated)")
        List<String> skipQueries = new ArrayList<>();

        @Option(names = "--concurrency", description = "Maximum number of queries to execute simultaneously. All --runs queries are submitted at once; each measures its own elapsed time independently. Default: ${DEFAULT-VALUE}. Incompatible with --gpu-memory-trace.")
        int concurrency = 1;

        @Option(names = {"-p", "--profile"}, description = "async-profiler event: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
        ProfileEvent profileEvent = ProfileEvent.NONE;

        @Option(names = {"-m", "--mode"}, description = "Execution backend: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
        ExecutionMode mode = ExecutionMode.CPU;

        @Option(names = "--data", description = "Data directory or URI (e.g. s3://bucket/prefix). Default: workload-specific.")
        String dataLocation;

        @Option(names = "--fs-cache", description = "Directory backing an OS-local filesystem cache for remote reads. When set, the workload enables fs.cache.* against this directory; reads are uncached when unset.")
        String fsCacheDirectory;

        @Option(names = "--debug", description = "Enable debug logging")
        boolean debug;

        /**
         * Parameters for {@code compute-sanitizer}. Useful parameters include
         * <ul>
         *     <li>--tool memcheck</li>
         *     <li>--report-api-errors no</li>
         *     <li>--leak-check full (memcheck) — report leaks at app exit</li>
         *     <li>--track-unused-memory yes (initcheck) — report device memory that was allocated but never read</li>
         *     <li>--print-limit {@code <N>} — cap reported errors</li>
         *     <li>--log-file {@code <path>} / --xml — redirect or format output</li>
         * </ul>
         */
        @Option(names = "--gpu-sanitizer",
                arity = "0..1",
                fallbackValue = "--tool memcheck",
                description = "Run the benchmark JVM under NVIDIA compute-sanitizer with the given arguments (e.g. \"--tool memcheck --leak-check full\"). Defaults to \"--tool memcheck\" when passed with no value. Requires --mode=gpu.")
        Optional<String> gpuSanitizer;

        @Option(names = "--nsys", description = "Run the benchmark JVM under nsys profile. Optional value is extra nsys arguments. Requires --mode=gpu.")
        boolean nsys;

        RunCommand(Launcher launcher, Workload workload)
        {
            this.launcher = requireNonNull(launcher, "launcher is null");
            this.workload = requireNonNull(workload, "workload is null");
        }

        @Override
        public Integer call()
                throws Exception
        {
            if (gpuSanitizer.isPresent() && nsys) {
                throw new IllegalArgumentException("--gpu-sanitizer and --nsys are mutually exclusive");
            }
            Optional<List<String>> launchWrapperCommand;
            if (gpuSanitizer.isPresent()) {
                String sanitizerArgs = gpuSanitizer.get();
                if (Boolean.getBoolean(NO_FORK_SYSTEM_PROPERTY)) {
                    throw new IllegalArgumentException("--gpu-sanitizer cannot be used with -D%s=true: there is no child process to wrap with compute-sanitizer".formatted(
                            NO_FORK_SYSTEM_PROPERTY));
                }
                if (mode != ExecutionMode.GPU) {
                    throw new IllegalArgumentException("--gpu-sanitizer is not useful without --mode=GPU");
                }
                checkArgument(!sanitizerArgs.contains("\"") && !sanitizerArgs.contains("'"), "Quotes are not supported in --gpu-sanitizer: %s", sanitizerArgs);
                launchWrapperCommand = Optional.of(Stream.concat(
                                Stream.of("compute-sanitizer"),
                                Splitter.on(" ").omitEmptyStrings().splitToStream(sanitizerArgs))
                        .toList());
            }
            else if (nsys) {
                if (Boolean.getBoolean(NO_FORK_SYSTEM_PROPERTY)) {
                    throw new IllegalArgumentException("--nsys cannot be used with -D%s=true: there is no child process to wrap with nsys".formatted(
                            NO_FORK_SYSTEM_PROPERTY));
                }
                if (mode != ExecutionMode.GPU) {
                    throw new IllegalArgumentException("--nsys is not useful without --mode=GPU");
                }
                if (queries.size() != 1) {
                    throw new IllegalArgumentException("--nsys requires exactly one --query to profile");
                }
                Path profileOutputDir = PROJECT_ROOT.resolve("target/benchmark-output").resolve(workload.name()).resolve("profile");
                Files.createDirectories(profileOutputDir);
                Path outputPath = profileOutputDir.resolve(workload.normalizeQuery(queries.getFirst()));
                launchWrapperCommand = Optional.of(buildNsysCommand(outputPath));
            }
            else {
                launchWrapperCommand = Optional.empty();
            }
            if (launcher.relaunchIfNeeded(launchWrapperCommand)) {
                return 0;
            }

            if (!queries.isEmpty() && !skipQueries.isEmpty()) {
                throw new IllegalArgumentException("--query and --skip-query are mutually exclusive");
            }
            if (concurrency < 1) {
                throw new IllegalArgumentException("--concurrency must be at least 1");
            }
            if (concurrency > runs) {
                throw new IllegalArgumentException(format("--concurrency (%d) must not exceed --runs (%d)", concurrency, runs));
            }

            if (debug) {
                enableDebugLogging();
            }

            String data = canonicalize(dataLocation != null ? dataLocation : workload.defaultDataLocation());
            if (dataLocation == null) {
                workload.validateDataLocation(data);
            }

            Path benchmarkDataDir = PROJECT_ROOT.resolve("target/benchmark-output").resolve(workload.name());
            Path profileOutputDir = benchmarkDataDir.resolve("profile");
            Path explainOutputDir = benchmarkDataDir.resolve("explain");
            Files.createDirectories(explainOutputDir);

            log.info("Per-iteration EXPLAIN ANALYZE plans will be written under %s", explainOutputDir.toAbsolutePath());

            try (DistributedQueryRunner runner = workload.createRunner(data, mode, /*bind8080*/ false, Optional.ofNullable(fsCacheDirectory).map(Path::of));
                    ListeningExecutorService executor = concurrency == 1 ? newDirectExecutorService() : listeningDecorator(newFixedThreadPool(concurrency))) {
                ProfileSession session = ProfileSession.of(profileEvent, workload, profileOutputDir);

                log.info("Running Trino at %s (mode=%s)", runner.getCoordinator().getBaseUrl(), mode);
                log.info("Running %s benchmark: %s suite warmup, %s warmup, %s measured runs, reporting average%s", workload.name(), suiteWarmup, warmup, runs, concurrency > 1 ? format(" (concurrency=%d)", concurrency) : "");
                if (dataLocation != null) {
                    workload.verifyDataset(runner);
                }
                verifyTableStatistics(runner, workload);

                Set<String> skipped = skipQueries.stream().map(workload::normalizeQuery).collect(toImmutableSet());
                List<String> queriesRun = queries.isEmpty()
                        ? workload.defaultQueries()
                        : queries.stream()
                          .map(workload::normalizeQuery)
                          .toList();

                List<String> suiteQueries = workload.defaultQueries();
                for (int round = 1; round <= suiteWarmup; round++) {
                    log.info("Suite prewarm round %d/%d (%d queries)", round, suiteWarmup, suiteQueries.size());
                    for (String query : suiteQueries) {
                        if (skipped.contains(query)) {
                            log.debug("Suite prewarm %s: skipped (--skip-query)", query);
                            continue;
                        }
                        log.debug("Starting warmup run of %s", query);
                        try {
                            runner.execute(workload.readQuery(query));
                        }
                        catch (RuntimeException e) {
                            e.addSuppressed(new Exception("Query: " + query));
                            if (!isOutOfMemory(e)) {
                                throw e;
                            }
                            log.warn(e, "Suite prewarm %s: out of memory — continuing", query);
                        }
                    }
                }

                session.writeRunMetadata(queriesRun, warmup, runs);

                Map<String, List<Measurement>> measurementsByQuery = new LinkedHashMap<>();
                long totalElapsedMillis = 0;
                long totalExecutionMillis = 0;
                for (String query : queriesRun) {
                    if (skipped.contains(query)) {
                        log.info("%s: skipped (--skip-query)", query);
                        measurementsByQuery.put(query, List.of());
                        continue;
                    }
                    List<Measurement> measurements = benchmarkQuery(runner, query, session, explainOutputDir, executor, concurrency);
                    measurementsByQuery.put(query, measurements);
                    if (!measurements.isEmpty()) {
                        totalElapsedMillis += averageMillis(measurements, Measurement::elapsedMillis);
                        totalExecutionMillis += averageMillis(measurements, Measurement::executionMillis);
                    }
                }
                log.info("Sum of averages for all queries: %s ms (execution %s ms)", totalElapsedMillis, totalExecutionMillis);
                writeTimingsCsv(benchmarkDataDir, measurementsByQuery);

                session.mergeCollapsedFiles(queriesRun);
            }

            return 0;
        }

        /**
         * Warmup runs before the profiler attaches so JIT compilation isn't in the hot frames.
         * Each measured iteration's result is compared against the workload's expected NDJSON;
         * mismatch fails the run. Memory-limit failures are logged and the query is skipped
         * (returns an empty list). Per-iteration EXPLAIN ANALYZE-equivalent plans are dumped to
         * {@code <explainOutputDir>/q<NN>.explain-analyze.txt}.
         */
        private List<Measurement> benchmarkQuery(
                DistributedQueryRunner runner,
                String query,
                ProfileSession session,
                Path explainOutputDir,
                ListeningExecutorService executor,
                int concurrency)
                throws IOException
        {
            String sql = workload.readQuery(query);
            List<String> expectedLines = readExpectedLines(workload, query);
            Path explainFile = explainOutputDir.resolve(query + ".explain-analyze.txt");

            List<Measurement> measurements = new ArrayList<>();
            List<IterationResult> measuredIterations = new ArrayList<>();
            List<Long> peakGpuBytesPerIter = new ArrayList<>();
            try (BufferedWriter explainWriter = Files.newBufferedWriter(explainFile, UTF_8)) {
                for (int i = 0; i < warmup; i++) {
                    log.debug("Starting warmup run of %s", query);
                    IterationResult warmupIteration = measureAndValidate(runner, sql, query, expectedLines);
                    log.debug("Warmup run of %s took %s ms", query, warmupIteration.measurement().elapsedMillis());
                }
                session.start();
                boolean trackGpuMemory = mode == ExecutionMode.GPU && concurrency == 1;
                boolean trackConcurrentGpuMemory = mode == ExecutionMode.GPU && concurrency > 1;
                List<ListenableFuture<IterationResult>> futures = new ArrayList<>(runs);
                if (trackConcurrentGpuMemory) {
                    Rmm.resetScopedMaximumBytesAllocated();
                }
                for (int i = 0; i < runs; i++) {
                    futures.add(executor.submit(() -> {
                        if (trackGpuMemory) {
                            Rmm.resetScopedMaximumBytesAllocated();
                        }
                        IterationResult iteration = measureAndValidate(runner, sql, query, expectedLines);
                        if (trackGpuMemory) {
                            peakGpuBytesPerIter.add(Rmm.getScopedMaximumBytesAllocated());
                        }
                        return iteration;
                    }));
                }
                List<IterationResult> iterationResults;
                try {
                    iterationResults = Futures.allAsList(futures).get();
                }
                catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    throw cause instanceof RuntimeException runtime ? runtime : new RuntimeException(cause);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for concurrent queries", e);
                }
                if (trackConcurrentGpuMemory) {
                    peakGpuBytesPerIter.add(Rmm.getScopedMaximumBytesAllocated());
                }
                for (IterationResult iteration : iterationResults) {
                    log.debug("Measured run of %s took %s ms", query, iteration.measurement().elapsedMillis());
                    measurements.add(iteration.measurement());
                    measuredIterations.add(iteration);
                }
                // Halt sampling before rendering plans so PlanPrinter frames don't pollute the
                // flamegraph; the buffer is preserved for the dump that runs after the try block.
                session.stop();
                for (int i = 0; i < measuredIterations.size(); i++) {
                    IterationResult iteration = measuredIterations.get(i);
                    explainWriter.write("=== %s run %d/%d ===%n".formatted(query, i + 1, measuredIterations.size()));
                    explainWriter.write(renderExplainAnalyze(runner.getCoordinator(), iteration.queryInfo()));
                    explainWriter.newLine();
                }
            }
            catch (RuntimeException e) {
                e.addSuppressed(new Exception("Query: " + query));
                try {
                    session.stop();
                }
                catch (Exception stopError) {
                    log.warn(stopError, "Profiler stop failed after %s error", query);
                }
                if (isOutOfMemory(e)) {
                    log.warn("%s: FAILED (out of memory) — skipping query", query);
                    return List.of();
                }
                throw e;
            }
            finally {
                session.dumpAndPostProcess(query);
            }

            long averageElapsed = averageMillis(measurements, Measurement::elapsedMillis);
            long averageExecution = averageMillis(measurements, Measurement::executionMillis);
            String peakSuffix = peakGpuBytesPerIter.isEmpty()
                    ? ""
                    : format(" [peak GPU memory: %s]", succinctBytes((long) peakGpuBytesPerIter.stream().mapToLong(Long::longValue).average().orElse(0)));
            log.info("Average for %s: %s ms (execution %s ms)%s", query, averageElapsed, averageExecution, peakSuffix);
            return List.copyOf(measurements);
        }
    }

    private static boolean isOutOfMemory(Throwable throwable)
    {
        QueryFailedException queryFailed = getCausalChain(throwable).stream()
                .filter(QueryFailedException.class::isInstance)
                .map(QueryFailedException.class::cast)
                .findFirst()
                .orElse(null);
        if (queryFailed == null) {
            return false;
        }
        for (Throwable current : getCausalChain(queryFailed.getCause())) {
            FailureInfo failureInfo = ((FailureException) current).getFailureInfo();
            if (ExceededMemoryLimitException.class.getName().equals(failureInfo.getType())) {
                // out of CPU memory
                return true;
            }
            if (OutOfMemoryError.class.getName().equals(failureInfo.getType()) &&
                    (nullToEmpty(failureInfo.getMessage()).startsWith("Could not allocate native memory: std::bad_alloc: out_of_memory: RMM failure") ||
                            nullToEmpty(failureInfo.getMessage()).startsWith("Could not allocate native memory: std::bad_alloc: out_of_memory: CUDA error"))) {
                // out of GPU memory
                return true;
            }
        }
        return false;
    }

    private interface ProfileSession
    {
        ProfileSession NOOP = new ProfileSession()
        {
            @Override
            public void writeRunMetadata(List<String> queriesRun, int warmup, int runs) {}

            @Override
            public void start() {}

            @Override
            public void stop() {}

            @Override
            public void dumpAndPostProcess(String displayName) {}

            @Override
            public void mergeCollapsedFiles(List<String> queriesRun) {}
        };

        static ProfileSession of(ProfileEvent profileEvent, Workload workload, Path profileOutputDir)
                throws IOException
        {
            List<ProfileSession> active = new ArrayList<>();

            if (profileEvent != ProfileEvent.NONE) {
                active.add(new AsyncProfileSession(AsyncProfiler.getInstance(), profileEvent, workload, profileOutputDir));
            }

            if (active.isEmpty()) {
                return NOOP;
            }
            return getOnlyElement(active);
        }

        void writeRunMetadata(List<String> queriesRun, int warmup, int runs)
                throws IOException;

        void start()
                throws IOException;

        void stop()
                throws IOException;

        void dumpAndPostProcess(String displayName)
                throws IOException;

        void mergeCollapsedFiles(List<String> queriesRun)
                throws IOException;
    }

    private static final class AsyncProfileSession
            implements ProfileSession
    {
        private final AsyncProfiler profiler;
        private final ProfileEvent profileEvent;
        private final Workload workload;
        private final Path profileOutputDir;
        private boolean running;

        AsyncProfileSession(AsyncProfiler profiler, ProfileEvent profileEvent, Workload workload, Path profileOutputDir)
                throws IOException
        {
            this.profiler = profiler;
            this.profileEvent = profileEvent;
            this.workload = workload;
            this.profileOutputDir = profileOutputDir;
            Files.createDirectories(profileOutputDir);
            log.info("Profiler output will be written to %s", profileOutputDir.toAbsolutePath());
            log.info("Profiler event: %s  interval=%s", profileEvent, workload.profileInterval());
        }

        @Override
        public void writeRunMetadata(List<String> queriesRun, int warmup, int runs)
                throws IOException
        {
            BenchmarkRunner.writeRunMetadata(profileOutputDir, workload, queriesRun, warmup, runs, profileEvent);
        }

        @Override
        public void start()
                throws IOException
        {
            profiler.execute("start,event=%s,interval=%s,alluser,jstackdepth=%d"
                    .formatted(profileEvent.name().toLowerCase(Locale.ROOT), workload.profileInterval(), PROFILE_STACK_DEPTH));
            running = true;
        }

        @Override
        public void stop()
                throws IOException
        {
            if (!running) {
                return;
            }
            profiler.execute("stop");
            running = false;
        }

        @Override
        public void dumpAndPostProcess(String displayName)
                throws IOException
        {
            Path snapshot = Files.createTempFile("benchmark-" + displayName + "-", ".collapsed");
            Path htmlFile = profileOutputDir.resolve(displayName + ".html");
            profiler.execute("dump,file=%s,collapsed,threads".formatted(snapshot.toAbsolutePath()));
            profiler.execute("dump,file=%s,flamegraph,threads".formatted(htmlFile.toAbsolutePath()));
            long kept = postProcessProfile(profileOutputDir, displayName, snapshot, profileEvent, workload);
            Files.delete(snapshot);
            log.info("Profiler output for %s post-processed (%d samples kept after idle filter)", displayName, kept);
        }

        @Override
        public void mergeCollapsedFiles(List<String> queriesRun)
                throws IOException
        {
            BenchmarkRunner.mergeCollapsedFiles(profileOutputDir, queriesRun);
        }
    }

    private static List<String> buildNsysCommand(Path outputPath)
    {
        return ImmutableList.<String>builder()
                .add("nsys")
                .add("profile")
                .add("--trace=cuda,nvtx,osrt")
                .add("--cuda-memory-usage=true")
                .add("--output=%s".formatted(outputPath.toAbsolutePath()))
                .add("--force-overwrite=true")
                .build();
    }

    @Command(
            name = "runner",
            mixinStandardHelpOptions = true,
            description = "Start query runner (workload-specific).")
    static final class QueryRunnerCommand
            implements Callable<Integer>
    {
        private final Launcher launcher;
        private final Workload workload;

        @Option(names = {"-m", "--mode"},
                description = "Execution backend: ${COMPLETION-CANDIDATES}. Default: ${DEFAULT-VALUE}.")
        ExecutionMode mode = ExecutionMode.CPU;

        QueryRunnerCommand(Launcher launcher, Workload workload)
        {
            this.launcher = requireNonNull(launcher, "launcher is null");
            this.workload = requireNonNull(workload, "workload is null");
        }

        @Override
        public Integer call()
                throws Exception
        {
            // TODO support compute-sanitizer
            if (launcher.relaunchIfNeeded(Optional.empty())) {
                return 0;
            }

            enableDebugLogging();
            String data = canonicalize(workload.defaultDataLocation());
            workload.validateDataLocation(data);
            try (DistributedQueryRunner queryRunner = workload.createRunner(data, mode, /*bind8080*/ true, Optional.empty())) {
                log.info("======== SERVER STARTED (%s) ========", mode);
                log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
                verifyTableStatistics(queryRunner, workload);

                // Query runner runs in the background. It will terminate the process when its shut down cleanly.
                while (true) {
                    Thread.sleep(Duration.ofDays(1));
                }
            }
        }
    }

    @Command(
            name = "generate",
            mixinStandardHelpOptions = true,
            description = "Generate the benchmark dataset into the data directory (workload-specific).")
    static final class GenerateCommand
            implements Callable<Integer>
    {
        private final Launcher launcher;
        private final Workload workload;

        @Option(names = "--data", description = "Target directory. Default: workload-specific.")
        Path dataLocation;

        GenerateCommand(Launcher launcher, Workload workload)
        {
            this.launcher = requireNonNull(launcher, "launcher is null");
            this.workload = requireNonNull(workload, "workload is null");
        }

        @Override
        public Integer call()
                throws Exception
        {
            if (launcher.relaunchIfNeeded(Optional.empty())) {
                return 0;
            }

            Path target = dataLocation != null ? dataLocation : Path.of(workload.defaultDataLocation());
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
        private final Launcher launcher;
        private final Workload workload;

        @Option(names = {"-q", "--query"}, description = "A specific query number (can be repeated)")
        List<String> queries = new ArrayList<>();

        @Option(names = "--data", description = "Data directory or URI (e.g. s3://bucket/prefix). Default: workload-specific.")
        String dataLocation;

        RecordCommand(Launcher launcher, Workload workload)
        {
            this.launcher = requireNonNull(launcher, "launcher is null");
            this.workload = requireNonNull(workload, "workload is null");
        }

        @Override
        public Integer call()
                throws Exception
        {
            if (launcher.relaunchIfNeeded(Optional.empty())) {
                return 0;
            }

            String data = canonicalize(dataLocation != null ? dataLocation : workload.defaultDataLocation());
            if (dataLocation == null) {
                workload.validateDataLocation(data);
            }
            try (DistributedQueryRunner runner = workload.createRunner(data, ExecutionMode.CPU, /*bind8080*/ false, Optional.empty())) {
                if (dataLocation != null) {
                    workload.verifyDataset(runner);
                }
                verifyTableStatistics(runner, workload);
                List<String> queriesRun = queries.isEmpty() ? workload.defaultQueries() : queries.stream().map(workload::normalizeQuery).toList();
                for (String query : queriesRun) {
                    Path target = recordTargetFor(workload.expectedResultResource(query));
                    String sql = workload.readQuery(query);
                    MaterializedResult result = runner.execute(sql);
                    Files.createDirectories(target.getParent());
                    writeNdjson(target, result);
                    log.info("Recorded %s -> %s (%d rows)", query, target, result.getRowCount());
                }
            }
            return 0;
        }
    }

    private record IterationResult(Measurement measurement, QueryInfo queryInfo) {}

    private static IterationResult measureAndValidate(
            DistributedQueryRunner runner,
            String sql,
            String displayName,
            List<String> expectedLines)
    {
        MaterializedResultWithQueryId withId = runner.executeWithQueryId(runner.getDefaultSession(), sql);
        QueryInfo queryInfo = runner.getCoordinator().getQueryManager().getFullQueryInfo(withId.queryId());
        var stats = queryInfo.getQueryStats();
        List<String> actualLines = formatNdjsonLines(withId.result());
        if (!actualLines.equals(expectedLines)) {
            throw new AssertionError(formatResultMismatch(displayName, expectedLines, actualLines));
        }
        return new IterationResult(
                new Measurement(stats.getElapsedTime().toMillis(), stats.getExecutionTime().toMillis()),
                queryInfo);
    }

    /**
     * Reproduces the textual output of {@code EXPLAIN ANALYZE} for a finished query without
     * re-executing it — same call chain as {@code ExplainAnalyzeOperator}, fed from the completed
     * {@link QueryInfo}.
     */
    private static String renderExplainAnalyze(TestingTrinoServer coordinator, QueryInfo queryInfo)
    {
        return PlanPrinter.textDistributedPlan(
                queryInfo.getStages().orElseThrow(() -> new IllegalStateException("Query has no stages: " + queryInfo.getQueryId())),
                queryInfo.getQueryStats(),
                coordinator.getPlannerContext().getMetadata(),
                coordinator.getPlannerContext().getFunctionManager(),
                queryInfo.getSession().toSession(coordinator.getSessionPropertyManager()),
                /* verbose = */ true,
                coordinator.getInstance(Key.get(NodeVersion.class)));
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
        flat.append(format(
                "# Raw samples: %d  Kept after filter: %d (%.1f%%)%n",
                totalSamples,
                keptSamples,
                100.0 * keptSamples / Math.max(1, totalSamples)));
        flat.append(format("# Event: %s  Interval: %s%n", profileEvent.name().toLowerCase(Locale.ROOT), workload.profileInterval()));
        flat.append(format("#%n"));
        flat.append(format("# By thread group (after filter):%n"));
        long keptForLambda = keptSamples;
        byThreadGroup.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .forEach(entry -> flat.append(format(
                        "#   %-16s %8d  %5.1f%%%n",
                        entry.getKey(),
                        entry.getValue(),
                        100.0 * entry.getValue() / Math.max(1, keptForLambda))));
        flat.append(format("#%n"));
        flat.append(format("%-10s %-8s %s%n", "samples", "pct", "leaf frame"));
        selfTime.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(30)
                .forEach(entry -> flat.append(format(
                        "%-10d %-7.2f%% %s%n",
                        entry.getValue(),
                        100.0 * entry.getValue() / Math.max(1, keptForLambda),
                        entry.getKey())));

        Files.writeString(profileOutputDir.resolve(displayName + ".flat.txt"), flat.toString(), UTF_8);
        return keptSamples;
    }

    /**
     * Concatenate per-query collapsed files into one cross-query file.
     */
    private static void mergeCollapsedFiles(Path profileOutputDir, List<String> queriesRun)
            throws IOException
    {
        Path merged = profileOutputDir.resolve("merged.collapsed");
        int mergedCount = 0;
        try (OutputStream out = Files.newOutputStream(merged)) {
            for (String query : queriesRun) {
                Path perQueryFile = profileOutputDir.resolve("%s.filtered.collapsed".formatted(query));
                if (!Files.exists(perQueryFile)) {
                    continue;
                }
                Files.copy(perQueryFile, out);
                mergedCount++;
            }
        }
        log.info("Merged collapsed stacks written to %s (%d per-query files merged)",
                merged,
                mergedCount);
    }

    /**
     * Write per-query timing data to {@code timings.csv} (overwrites any previous file —
     * copy or rename it to keep history across runs).
     */
    private static void writeTimingsCsv(
            Path benchmarkDataDir,
            Map<String, List<Measurement>> measurementsByQuery)
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
            for (Map.Entry<String, List<Measurement>> entry : measurementsByQuery.entrySet()) {
                writer.write(entry.getKey());
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
            List<String> queriesRun,
            int warmup,
            int runs,
            ProfileEvent profileEvent)
            throws IOException
    {
        List<String> jvmArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        Runtime runtime = Runtime.getRuntime();
        String content =
                """
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
            new ThreadGroupRule(
                    "task-runner",
                    Pattern.compile("TimeSharingTaskExecutor\\$TaskRunner|PrioritizedSplitRunner")),
            new ThreadGroupRule(
                    "compiler",
                    Pattern.compile("CompileBroker::compiler_thread_loop|C1Compiler|C2Compiler")),
            new ThreadGroupRule(
                    "gc-vm",
                    Pattern.compile("G1ConcurrentRefine|G1ParScanThreadState|GCTaskThread|VMThread::run")),
            new ThreadGroupRule(
                    "jetty",
                    Pattern.compile("ServerConnector|HttpChannelOverHttp|ReservedThread")),
            new ThreadGroupRule(
                    "jvm-internal",
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

    private static List<String> readExpectedLines(Workload workload, String query)
            throws IOException
    {
        String resource = workload.expectedResultResource(query);
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
        Session withoutGpu = Session.builder(runner.getDefaultSession())
                .setSystemProperty(GPU_EXECUTION_ENABLED, "false")
                .build();
        log.info("Verifying table statistics for %d table(s): %s", tables.size(), tables);
        for (String table : tables) {
            Optional<String> missing = findMissingStatistic(runner, withoutGpu, table);
            if (missing.isEmpty()) {
                log.info("  %s: statistics present", table);
                continue;
            }
            log.info("  %s: missing %s, running ANALYZE", table, missing.get());
            long start = System.nanoTime();
            runner.execute(withoutGpu, "ANALYZE " + table);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            log.info("  %s: ANALYZE completed in %d ms", table, elapsedMillis);
        }
    }

    private static Optional<String> findMissingStatistic(DistributedQueryRunner runner, Session session, String table)
    {
        MaterializedResult result = runner.execute(session, "SHOW STATS FOR " + table);
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

    public static boolean isRemote(String dataLocation)
    {
        return dataLocation.contains("://") && !dataLocation.startsWith("file:");
    }

    static String canonicalize(String dataLocation)
    {
        if (isRemote(dataLocation)) {
            return dataLocation.replaceFirst("/$", "");
        }
        return Path.of(dataLocation).toAbsolutePath().normalize().toString();
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
     * Apply deterministic-write settings (single writer per task, no scaling).
     */
    public static <T extends DistributedQueryRunner.Builder<T>> T applyDataGenerationConfiguration(T builder)
    {
        return builder
                .setWorkerCount(0)
                .addExtraProperties(ImmutableMap.<String, String>builder()
                        .put("scale-writers", "false")
                        .put("task.scale-writers.enabled", "false")
                        .put("task.max-writer-count", "1")
                        .put("query.max-writer-task-count", "1")
                        .put("redistribute-writes", "false")
                        .buildOrThrow());
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
     * Apply connector-agnostic extras (resource sizing, GPU acceleration toggles, single-node, etc.).
     */
    public static void configureQueryRunner(DistributedQueryRunner.Builder<?> builder, ExecutionMode mode, boolean bind8080)
    {
        // TestingTrinoServer sets both task.concurrency and task.max-worker-threads to 4.
        // Here, we override them with the default values, which are determined based on the hardware
        // on which the benchmarks run.
        TaskManagerConfig taskManagerDefaults = new TaskManagerConfig();
        int taskConcurrency = taskManagerDefaults.getTaskConcurrency();
        int maxWorkerThreads = taskManagerDefaults.getMaxWorkerThreads();
        builder.addExtraProperty("task.concurrency", Integer.toString(taskConcurrency));
        builder.addExtraProperty("task.max-worker-threads", Integer.toString(maxWorkerThreads));
        builder.addExtraProperty("query.max-memory-per-node", "80%");
        builder.addExtraProperty("query.max-memory", "1TB");
        builder.addExtraProperty("memory.heap-headroom-per-node", "20%");
        builder.addExtraProperty("experimental.force-single-node-query", "true");
        // Restore the default overridden by DistributedQueryRunner.
        builder.addExtraProperty("join-distribution-type", new OptimizerConfig().getJoinDistributionType().name());
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
        }
        switch (mode) {
            case CPU -> builder
                    .addExtraProperty("gpu-execution", "false");
            case GPU -> builder
                    .addExtraProperty("gpu-execution", "true")
                    .addExtraProperty("task.gpu-execution.enabled", "true");
        }
    }

    // Configures an OS-local filesystem cache for a catalog's remote reads. No-op when the directory is absent.
    // The cache manager must be loaded before the catalog using it is created, so use this overload when the
    // catalog is created by the builder and the overload below when the catalog is created on a running server.
    public static void applyFilesystemCache(DistributedQueryRunner.Builder<?> builder, Map<String, String> catalogProperties, Optional<Path> fsCacheDirectory)
    {
        fsCacheDirectory.ifPresent(directory -> {
            catalogProperties.put("fs.cache.enabled", "true");
            builder.withPlugin(new AlluxioBlobCachePlugin());
            builder.withBlobCache("alluxio", blobCacheProperties(directory));
        });
    }

    public static void applyFilesystemCache(DistributedQueryRunner runner, Map<String, String> catalogProperties, Optional<Path> fsCacheDirectory)
    {
        fsCacheDirectory.ifPresent(directory -> {
            catalogProperties.put("fs.cache.enabled", "true");
            runner.installPlugin(new AlluxioBlobCachePlugin());
            runner.loadBlobCacheManager("alluxio", blobCacheProperties(directory));
        });
    }

    private static Map<String, String> blobCacheProperties(Path directory)
    {
        return ImmutableMap.of(
                "fs.cache.directories", directory.toString(),
                "fs.cache.max-sizes", FS_CACHE_MAX_SIZE);
    }

    static void enableDebugLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.spi.gpu", Level.DEBUG);
        logging.setLevel("io.trino.operator.gpu", Level.DEBUG);
        logging.setLevel("io.trino.sql.planner.LocalExecutionPlanner", Level.DEBUG);
        logging.setLevel("io.trino.split.PageSourceManager", Level.DEBUG);
        logging.setLevel("io.trino.tests.benchmark", Level.DEBUG);
        logging.setLevel("io.trino.plugin.hive.HivePageSourceProvider", Level.DEBUG);
        logging.setLevel("io.trino.plugin.iceberg.IcebergPageSourceProvider", Level.DEBUG);
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
}
